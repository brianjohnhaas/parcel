from .portability import OS_WINDOWS
from intervaltree import Interval, IntervalTree
import os
import pickle
import random
import string
import tempfile
import time

if OS_WINDOWS:
    WINDOWS = True
    from Queue import Queue
else:
    # if we are running on a posix system, then we will be
    # communicating across processes, and will need
    # multiprocessing manager
    from multiprocessing import Manager
    WINDOWS = False

from log import get_logger
from utils import get_pbar, md5sum, mmap_open, STRIP
from const import SAVE_INTERVAL
from progressbar import ProgressBar, Percentage, Bar, ETA

log = get_logger('segment')
log.info("segment info msg")
log.warning("segment warning msg")
log.debug("segment debug msg")

class SegmentProducer(object):

    save_interval = SAVE_INTERVAL

    def __init__(self, download, n_procs):

        assert download.size is not None,\
            'Segment producer passed uninitizalied Download!'

        self.download = download
        self.n_procs = n_procs

        # Initialize producer
        self.load_state()
        self._setup_pbar()
        self._setup_queues()
        self._setup_work()
        self.schedule()

    def _setup_pbar(self):
        self.pbar = None
        self.pbar = get_pbar(self.download.ID, self.download.size)

    def _setup_work(self):

        work_size = self.integrate(self.work_pool)
        self.block_size = work_size / self.n_procs

        if self.is_complete():
            log.info('File already complete.')
            return



    def _setup_queues(self):
        if WINDOWS:
            self.q_work = Queue()
            self.q_complete = Queue()
        else:
            manager = Manager()
            self.q_work = manager.Queue()
            self.q_complete = manager.Queue()

    def integrate(self, itree):

        intervals = itree.items()

        ## because of overlap, we must take those overlapping regions into account
        def __sort_intervals (a,b):                                                                                                                  
            if a.begin > b.begin:
                return 1
            elif a.begin == b.begin:
                if a.end > b.end:
                    return 1
                elif a.end < b.end:                                                                                         
                    return -1
                else:
                    return 0                                                                                              
            else:
                return -1      
            
        intervals = sorted(intervals, cmp=__sort_intervals)
        
        if (0):  ## debugging, bhaas, found some intervals that overlapped, having same start position but different end positions.
            log.debug("writing itree.spans file info")
            with open('itree.spans', 'w') as ofh:
                for i in intervals:
                    ofh.write("\t".join([str(i.begin), str(i.end)]) + "\n")
                    log.debug("interval: " + "\t".join([str(i.begin), str(i.end)]))

        #interval_sum = sum([i.end-i.begin for i in itree.items()])

        interval_sum = 0
        prev_end = -1
        for i in intervals:
            interval_len = i.end - i.begin

            if prev_end > 0 and prev_end > i.begin:
                interval_len = i.end - prev_end

            interval_sum += interval_len
            prev_end = i.end
        
            


        log.debug("interval sum: {}".format(interval_sum))
        
        return interval_sum
    
    

    def validate_segment_md5sums(self):
        if not self.download.check_segment_md5sums:
            return True
        corrupt_segments = 0
        intervals = sorted(self.completed.items())
        pbar = ProgressBar(widgets=[
            'Checksumming {}: '.format(self.download.ID), Percentage(), ' ',
            Bar(marker='#', left='[', right=']'), ' ', ETA()])
        with mmap_open(self.download.path) as data:
            for interval in pbar(intervals):
                log.debug('Checking segment md5: {}'.format(interval))
                if not interval.data or 'md5sum' not in interval.data:
                    log.error(STRIP(
                        """User opted to check segment md5sums on restart.
                        Previous download did not record segment
                        md5sums (--no-segment-md5sums)."""))
                    return
                chunk = data[interval.begin:interval.end]
                checksum = md5sum(chunk)
                if checksum != interval.data.get('md5sum'):
                    log.debug('Redownloading corrupt segment {}, {}.'.format(
                        interval, checksum))
                    corrupt_segments += 1
                    self.completed.remove(interval)
        if corrupt_segments:
            log.warn('Redownloading {} currupt segments.'.format(
                corrupt_segments))

    def load_state(self):
        # Establish default intervals
        self.work_pool = IntervalTree([Interval(0, self.download.size)])
        self.completed = IntervalTree()
        self.size_complete = 0
        if not os.path.isfile(self.download.state_path)\
           and os.path.isfile(self.download.path):
            log.warn(STRIP(
                """A file named '{} was found but no state file was found at at
                '{}'. Either this file was downloaded to a different
                location, the state file was moved, or the state file
                was deleted.  Parcel refuses to claim the file has
                been successfully downloaded and will restart the
                download.\n""").format(
                    self.download.path, self.download.state_path))
            return

        if not os.path.isfile(self.download.state_path):
            self.download.setup_file()
            return

        # If there is a file at load_path, attempt to remove
        # downloaded sections from work_pool
        log.info('Found state file {}, attempting to resume download'.format(
            self.download.state_path))

        if not os.path.isfile(self.download.path):
            log.warn(STRIP(
                """State file found at '{}' but no file for {}.
                Restarting entire download.""".format(
                    self.download.state_path, self.download.ID)))
            return
        try:
            with open(self.download.state_path, "rb") as f:
                self.completed = pickle.load(f)
            assert isinstance(self.completed, IntervalTree), \
                "Bad save state: {}".format(self.download.state_path)
        except Exception as e:
            self.completed = IntervalTree()
            log.error('Unable to resume file state: {}'.format(str(e)))
        else:
            log.debug("validating segment md5sums")
            self.validate_segment_md5sums()
            self.size_complete = self.integrate(self.completed)
            log.debug("size_complete: {}".format(self.size_complete))
            for interval in self.completed:
                log.debug("chopping completed intervals: {} - {}".format(interval.begin, interval.end))
                self.work_pool.chop(interval.begin, interval.end)
            log.debug("work pool left: {}".format(self.work_pool))
            

    def save_state(self):
        log.debug("SAVING STATE()")
        
        try:
            # Grab a temp file in the same directory (hopefully avoud
            # cross device links) in order to atomically write our save file
            temp = tempfile.NamedTemporaryFile(
                prefix='.parcel_',
                dir=os.path.abspath(self.download.state_directory),
                delete=False)
            # Write completed state
            pickle.dump(self.completed, temp)
            # Make sure all data is written to disk
            temp.flush()
            os.fsync(temp.fileno())
            temp.close()

            # Rename temp file as our save file, this could fail if
            # the state file and the temp directory are on different devices
            if OS_WINDOWS and os.path.exists(self.download.state_path):
                # If we're on windows, there's not much we can do here
                # except stash the old state file, rename the new one,
                # and back up if there is a problem.
                old_path = os.path.join(tempfile.gettempdir(), ''.join(
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in range(10)))
                try:
                    # stash the old state file
                    os.rename(self.download.state_path, old_path)
                    # move the new state file into place
                    os.rename(temp.name, self.download.state_path)
                    # if no exception, then delete the old stash
                    os.remove(old_path)
                except Exception as msg:
                    log.error('Unable to write state file: {}'.format(msg))
                    try:
                        os.rename(old_path, self.download.state_path)
                    except:
                        pass
                    raise
            else:
                # If we're not on windows, then we'll just try to
                # atomically rename the file
                os.rename(temp.name, self.download.state_path)

        except KeyboardInterrupt:
            log.warn('Keyboard interrupt. removing temp save file'.format(
                temp.name))
            temp.close()
            os.remove(temp.name)
        except Exception as e:
            log.error('Unable to save state: {}'.format(str(e)))
            raise

    def schedule(self):
        while True:
            interval = self._get_next_interval()
            log.debug('Schedule: interval: {}'.format(interval))
            if not interval:
                log.debug("done setting up scheduler.")
                return
            self.q_work.put(interval)

    def _get_next_interval(self):
        intervals = sorted(self.work_pool.items())
        if not intervals:
            return None
        interval = intervals[0]
        start = interval.begin
        end = min(interval.end, start + self.block_size)
        self.work_pool.chop(start, end)
        return Interval(start, end)

    def print_progress(self):
        if not self.pbar:
            return
        try:
            self.pbar.update(self.size_complete)
        except Exception as e:
            log.debug('Unable to update pbar: {}'.format(str(e)))

    def check_file_exists_and_size(self):
        if self.download.is_regular_file:

            
            return (os.path.isfile(self.download.path)
                    and os.path.getsize(
                        self.download.path) == self.download.size)
        else:
            log.debug('File is not a regular file, refusing to check size.')
            return (os.path.exists(self.download.path))

    def is_complete(self):
        log.debug("is_complete(): self.integrate(self.completed): {}, self.download.size: {}".format(self.integrate(self.completed), self.download.size))

        log.debug("check_file_exists_and_size: size so far: {}, and expected size: {}".format(os.path.getsize(self.download.path),
                                                                                                  self.download.size))

        

        return (self.integrate(self.completed) == self.download.size and
                self.check_file_exists_and_size())

    
    def finish_download(self):
        # Tell the children there is no more work, each child should
        # pull one NoneType from the queue and exit
        for i in range(self.n_procs):
            self.q_work.put(None)

        # Wait for all the children to exit by checking to make sure
        # that everyone has taken their NoneType from the queue.
        # Otherwise, the segment producer will exit before the
        # children return, causing them to read from a closed queue
        log.debug('Waiting for children to report')
        while not self.q_work.empty():
            time.sleep(0.1)

        # Finish the progressbar
        if self.pbar:
            self.pbar.finish()

    def wait_for_completion(self):

        log.debug("WAIT_FOR_COMPLETION()")
        
        try:
            since_save = 0
            while not self.is_complete():
                while since_save < self.save_interval:
                    log.debug("wait_for_completion loop: since_save: {}, save_interval: {}".format(since_save, self.save_interval))
                    interval = self.q_complete.get()
                    log.debug("wait_for_completion loop: got completed interval: {}".format(interval))
                    self.completed.add(interval)
                    if self.is_complete():
                        break
                    this_size = interval.end - interval.begin
                    self.size_complete += this_size
                    since_save += this_size
                    self.print_progress()
                since_save = 0
                self.save_state()
        finally:
            self.finish_download()  ## adds None values to work queues so all children exit
