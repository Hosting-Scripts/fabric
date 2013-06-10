"""
Sliding-window-based job/task queue class (& example of use.)

May use ``multiprocessing.Process`` or ``threading.Thread`` objects as queue
items, though within Fabric itself only ``Process`` objects are used/supported.
"""

from __future__ import with_statement
import time
import Queue

from collections import deque
from progressbar import Bar, ETA, Percentage, ProgressBar, SimpleProgress

from fabric.context_managers import settings
from fabric.network import ssh


class JobQueue(object):
    """
    The goal of this class is to make a queue of processes to run, and go
    through them running X number at any given time.

    So if the bubble is 5 start with 5 running and move the bubble of running
    procs along the queue looking something like this:

        Start
        ...........................
        [~~~~~]....................
        ___[~~~~~].................
        _________[~~~~~]...........
        __________________[~~~~~]..
        ____________________[~~~~~]
        ___________________________
                                End
    """
    def __init__(self, max_running, comms_queue, role_limits=None, debug=False):
        """
        Setup the class to resonable defaults.
        """
        self._max = max_running
        self._comms_queue = comms_queue
        self._debug = debug

        if role_limits is None:
            role_limits = {}
        role_limits.setdefault('default', self._max)

        self._pools = {}
        for role, limit in role_limits.iteritems():
            self._pools[role] = {
                'running': [],
                'queue': deque(),
                'limit': limit,
            }

        self._completed = []
        self._num_of_jobs = 0
        self._finished = False
        self._closed = False

        widgets = ['Running tasks: ', Percentage(), ' ', Bar(), ' ', SimpleProgress(), ' ', ETA()]
        self.pbar = ProgressBar(widgets=widgets)

    def _all_alive(self):
        """
        Simply states if all procs are alive or not. Needed to determine when
        to stop looping, and pop dead procs off and add live ones.
        """
        if self._running:
            for pool in self._pools.itervalues():
                if not all(x.is_alive() for x in pool['running']):
                    return False
            return True
        else:
            return False

    def __len__(self):
        """
        Just going to use number of jobs as the JobQueue length.
        """
        return self._num_of_jobs

    def close(self):
        """
        A sanity check, so that the need to care about new jobs being added in
        the last throws of the job_queue's run are negated.
        """
        if self._debug:
            print("JOB QUEUE: closed")

        self._closed = True

    def append(self, process):
        """
        Add the Process() to the queue, so that later it can be checked up on.
        That is if the JobQueue is still open.

        If the queue is closed, this will just silently do nothing.

        To get data back out of this process, give ``process`` access to a
        ``multiprocessing.Queue`` object, and give it here as ``queue``. Then
        ``JobQueue.run`` will include the queue's contents in its return value.
        """
        if not self._closed:
            r = process.name.split('|')[0]
            role = r if r in self._pools else 'default'

            self._pools[role]['queue'].appendleft(process)

            self._num_of_jobs += 1

            self.pbar.maxval = self._num_of_jobs

            if self._debug:
                print("JOB QUEUE: %s: added %s" % (role, process.name))

    def run(self):
        """
        This is the workhorse. It will take the intial jobs from the _queue,
        start them, add them to _running, and then go into the main running
        loop.

        This loop will check for done procs, if found, move them out of
        _running into _completed. It also checks for a _running queue with open
        spots, which it will then fill as discovered.

        To end the loop, there have to be no running procs, and no more procs
        to be run in the queue.

        This function returns an iterable of all its children's exit codes.
        """
        def _advance_the_queue(pool):
            """
            Helper function to do the job of poping a new proc off the queue
            start it, then add it to the running queue. This will eventually
            depleate the _queue, which is a condition of stopping the running
            while loop.

            It also sets the env.host_string from the job.name, so that fabric
            knows that this is the host to be making connections on.
            """
            job = pool['queue'].pop()
            if self._debug:
                print("Popping '%s' off the queue and starting it" % job.name)
            with settings(clean_revert=True, host_string=job.name, host=job.name):
                job.start()
            pool['running'].append(job)

        # Prep return value so we can start filling it during main loop
        results = {}
        for pool in self._pools.itervalues():
            for job in pool['queue']:
                # job.name contains role so split that off and discard
                job_name = job.name.split('|')[-1]
                results[job_name] = dict.fromkeys(('exit_code', 'results'))

        if not self._closed:
            raise Exception("Need to close() before starting.")

        if self._debug:
            print("JOB QUEUE: starting")

        self.pbar.start()

        while len(self._completed) < self._num_of_jobs:
            for pool_name, pool in self._pools.iteritems():
                while len(pool['queue']) and len(pool['running']) < pool['limit']:
                    _advance_the_queue(pool)

                for i, job in enumerate(pool['running']):
                    if not job.is_alive():
                        if self._debug:
                            print("JOB QUEUE: %s: %s: finish" % (pool_name, job.name))

                        job.join()  # not necessary for Process but is for Thread
                        self._completed.append(job)
                        pool['running'].pop(i)

                        job_name = job.name.split('|')[-1]
                        results[job_name]['exit_code'] = job.exitcode

                        # Each loop pass, try pulling results off the queue to keep its
                        # size down. At this point, we don't actually care if any results
                        # have arrived yet; they will be picked up after the main loop.
                        self._fill_results(results)

                        time.sleep(ssh.io_sleep)

                if self._debug:
                    print("JOB QUEUE: %s: %d running jobs" % (pool_name, len(pool['running'])))

                    if len(pool['queue']) == 0:
                        print("JOB QUEUE: %s: depleted" % pool_name)

            # Allow some context switching
            time.sleep(ssh.io_sleep)

            self.pbar.update(len(self._completed))

        # Consume anything left in the results queue. Note that there is no
        # need to block here, as the main loop ensures that all workers will
        # already have finished.
        self._fill_results(results)

        # Attach exit codes now that we're all done & have joined all jobs
        for job in self._completed:
            results[job.name]['exit_code'] = job.exitcode

        self.pbar.finish()

        return results

    def _fill_results(self, results):
        """
        Attempt to pull data off self._comms_queue and add to 'results' dict.
        If no data is available (i.e. the queue is empty), bail immediately.
        """
        while True:
            try:
                datum = self._comms_queue.get_nowait()
                results[datum['name']]['results'] = datum['result']
            except Queue.Empty:
                break


#### Sample

def try_using(parallel_type):
    """
    This will run the queue through it's paces, and show a simple way of using
    the job queue.
    """

    def print_number(number):
        """
        Simple function to give a simple task to execute.
        """
        print(number)

    if parallel_type == "multiprocessing":
        from multiprocessing import Process as Bucket  # noqa

    elif parallel_type == "threading":
        from threading import Thread as Bucket  # noqa

    # Make a job_queue with a bubble of len 5, and have it print verbosely
    jobs = JobQueue(5)
    jobs._debug = True

    # Add 20 procs onto the stack
    for x in range(20):
        jobs.append(Bucket(
            target=print_number,
            args=[x],
            kwargs={},
        ))

    # Close up the queue and then start it's execution
    jobs.close()
    jobs.run()


if __name__ == '__main__':
    try_using("multiprocessing")
    try_using("threading")
