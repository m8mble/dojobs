#!/usr/bin/env python3

import argparse
import collections
from datetime import datetime
import queue
import threading
import time
from . import execute


def _get_locked(lock, data, key):
    ''' Access data[key] while holding lock '''
    with lock:  # lock while obtaining key-specific data
        return data[key]


def _wait_to_start(wait_secs, host, data_lock, lock_data):
    ''' Wait some time to ensure host-jobs are not started more frequently than every wait_secs sec(s). '''
    if not wait_secs:
        return  # skip lock interactions entirely.
    with _get_locked(data_lock, lock_data, host):
        time.sleep(wait_secs)


#
# Function run by worker processes
#
def worker(host, setup_details, in_queue, out_queue):
    for job_id, job in iter(in_queue.get, 'STOP'):
        # Wait some time to ensure host-jobs are not started too frequently.
        _wait_to_start(host=host, **setup_details)
        start_date = datetime.now()

        # Execute job
        return_code, console = utils.execute.execute(job)

        # Write result to out_queue
        finish_date = datetime.now()
        out_queue.put({
            'job_id': job_id,
            'job': job,
            'return_code': return_code,
            'console': console,
            'host': host,
            'start_date': start_date,
            'finish_date': finish_date,
            'wall_time': finish_date - start_date,
        })


def report(job, job_id, return_code, host, wall_time, start_date, finish_date, console, suppress_console, **kwargs):
    prefix = '[{:d}]'.format(job_id)
    print(prefix, 'Job:        ', job)
    print(prefix, 'Return Code:', return_code)
    print(prefix, 'Host:       ', host)
    print(prefix, 'Wall Time:  ', wall_time, '(From {s} until {f})'.format(s=start_date, f=finish_date))
    if not suppress_console:
        print(prefix, 'Log: ', *console)


class _StoreHostDetails(argparse.Action):
    """ Helper to parse host specifications as pairs of host and num_threads. """

    def __init__(self, option_strings, dest, **kwargs):
        super(_StoreHostDetails, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        assert values, 'At least one argument is required.'
        assert isinstance(values, list), 'Need input arguments as list.'
        base_description = 'Specify with \'--host [USER@]HOST [NUM_THREADS]\''
        if len(values) > 2:
            parser.error('{d}; unsupported number of arguments!'.format(d=base_description))
        elif len(values) == 2:
            if not values[1].isdigit():
                parser.error('{d}; NUM_THREADS specified as \'{t}\' but needs to be integer.'.format(
                    d=base_description, t=values[1]))
            values[1] = int(values[1])
            if values[1] < 1:
                parser.error('{d}; non-positive NUM_THREADS specified ({t}).'.format(d=base_description, t=values[1]))
            values = tuple(values)
        else:  # len(values) == 1
            values = (values[0], 10)
        getattr(namespace, self.dest).append(values)


# Adapted using an official multiprocessing doc example (ie. the last one).
def main():
    parser_config = {
        # TODO: Host specification
        ('--jobfile', '-j'):
            {'type': argparse.FileType('r'), 'help': 'Path to file containing all jobs.', 'required': True},
        ('--host',):
            {'metavar': 'H', 'action': _StoreHostDetails, 'nargs': '+', 'default': [],
             'help': 'Add host H as worker for jobs.'},
        ('--suppress-console',): {'action': 'store_true', 'help': 'Suppress job output.'},
        ('--setup-pause',): {'type': float, 'help': 'Time to wait before starting a job in sec.'},
        ('--list-arguments',): {'action': 'store_true', 'help': 'List allowed arguments (for auto-completion).'},
    }
    parser = argparse.ArgumentParser(
        description='Run jobs (remotely) in parallel.',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog='''
A job is a one-line bash-like command. The job will not be executed
in a full shell, ie. with restricted features only. Output
redirection will not work, while chaining with '&&' or ';' is
supported.

A jobfile is a file containing one job in each line. Comments as well
as empty lines are not supported.

Hosts are specified as '[user@]host [num_threads]'. This adds
'num_threads' workers on given 'host' to execute jobs in parallel.
Both 'user' and 'num_threads' are optional. If 'num_threads' is given
it must be integral and positive; otherwise its value defaults to the
maximum number of cores available on 'host'.
''')
    for options, config in parser_config.items():
        parser.add_argument(*options, **config)

    args = parser.parse_args()

    # TODO: Adjust specified numbers acc. to max number of free cores. Make adjusting default, add option to turn off.
    # TODO: Add special handling for localhost, ie. execute right here, right now.
    work_hosts = args.host
    num_processes = sum([x for (d, x) in work_hosts])

    setup_details = {
        'wait_secs': args.setup_pause,
        'data_lock': threading.Lock(),
        'lock_data': collections.defaultdict(threading.Lock)
    }

    # Create queues
    task_queue = queue.Queue()
    done_queue = queue.Queue()

    # Submit tasks
    for job_id, job in enumerate(args.jobfile):
        job = job.strip()  # Remove trailing whitespace, in particular the potential '\n'.
        task_queue.put((job_id, job))
        num_jobs = job_id + 1  # Queue has no len, hence we have to keep track manually.

    # Setup workers
    # TODO: Can be encapsulated in a with WorkerContext class?
    for host, num in work_hosts:
        for i in range(num):
            threading.Thread(target=worker, args=(host, setup_details, task_queue, done_queue)).start()

    # Get and report results
    for i in range(num_jobs):
        report(suppress_console=args.suppress_console, **done_queue.get())

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

# TODO: How does STOP work? How does iter work?
# TODO: Documentation

