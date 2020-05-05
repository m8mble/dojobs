import logging
import pipes
import subprocess
import enum
from datetime import datetime


def enable_debug_logging():
    logging.getLogger().setLevel(logging.DEBUG)


def ssh_prefix(host):
    return ['ssh', '-o', 'StrictHostKeyChecking no', host]


def rsync_prefix():
    return ['rsync',
            '--outbuf', 'L',  # Enable per line buffering
            '--links', '--recursive', '--perms', '--group', '--verbose',  # just the usual
            '-e', 'ssh -o "StrictHostKeyChecking no"']  # accept anything, just continue


ExecutePrintMode = enum.Enum('ExecutePrintMode',
                             ['with_timestamps_and_prefix', 'with_prefix', 'pure_lines', 'no_printing'])


def execute(input_cmd, show_errors=False, cwd=None, print_prefix=None, timeout=None, print_mode=None):
    ''' Execute given cmd (list of strings), ignore errors and return the cmd output.
        Note: The cmd will not be run inside a shell, ie. shell features (ie. redirection) are not available.
       :param input_cmd: List of strings forming the cmd to be executed.
       :param show_errors: Toggle whether errors of the executed cmd are suppressed.
       :param cwd: Folder where the given cmd should be executed in.
       :param print_mode: How the output is printed. Specifying a print_prefix will enable with_timestamps_and_prefix
          mode. Otherwise nothing will be printed by default.
       :return: tuple of return code and list of output lines. The list is returned even if the output was also printed.
    '''

    def print_line_with_timestamp(line):
        n = datetime.now().time()
        print('[{:s} {:02d}:{:02d}:{:02d}] {:s}'.format(print_prefix, n.hour, n.minute, n.second, line), end='')

    available_printers = {
        ExecutePrintMode.with_timestamps_and_prefix: lambda l: print_line_with_timestamp(line),
        ExecutePrintMode.with_prefix: lambda l: print('[{:s}] {:s}'.format(print_prefix, line), end=''),
        ExecutePrintMode.pure_lines: lambda l: print(str(l), end=''),
        ExecutePrintMode.no_printing: lambda l: None
    }

    # For historical reasons
    if print_prefix is not None and print_mode is None:
        print_mode = ExecutePrintMode.with_timestamps_and_prefix
    if print_mode is None:
        print_mode = ExecutePrintMode.no_printing
    assert isinstance(print_mode, ExecutePrintMode), 'Need to specify a valid ExecutePrintMode!'
    if (print_mode == ExecutePrintMode.with_prefix or print_mode == ExecutePrintMode.with_timestamps_and_prefix) \
            and (print_prefix is None or len(print_prefix) == 0):
        print_mode = ExecutePrintMode.pure_lines
    assert print_mode is not None, 'Need a valid ExecutePrintMode at this point!'

    cmd = input_cmd
    logging.debug('Ex : ' + ' '.join([c if c[0] == '-' else pipes.quote(c) for c in cmd]))

    try:
        stderr = None if show_errors else subprocess.DEVNULL
        # return subprocess.check_output(cmd, universal_newlines=True, stderr=stderr, cwd=cwd, timeout=timeout).splitlines()
        printer = available_printers[print_mode]
        proc = subprocess.Popen(cmd, universal_newlines=True, stdout=subprocess.PIPE, stderr=stderr, bufsize=1, cwd=cwd)
        str_data = []
        for line in proc.stdout:
            str_data.append(str(line).strip('\n'))
            printer(line)
        proc.communicate(timeout=timeout)  # Should do just nothing, but ensures proc is actually done
        return proc.returncode, str_data
    except subprocess.CalledProcessError as xcp:
        return xcp.returncode, xcp.output.splitlines()
    except FileNotFoundError:
        return 1, []  # specified cwd might not exist (locally)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        return 2, []  # just return nothing
