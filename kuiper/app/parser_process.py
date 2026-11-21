"""Run one parser in a separate process and stream its record batches."""

import importlib
import os
import pickle
import signal
import sys
from itertools import islice


def write_batches(records, size, output):
    iterator = iter(records)
    while True:
        batch = list(islice(iterator, size))
        if not batch:
            return
        pickle.dump(batch, output, protocol=2)
        output.flush()


def run(module_name, function_name, artifact, parser_name, batch_size, output):
    module = importlib.import_module(module_name)
    records = getattr(module, function_name)(file=artifact, parser=parser_name)
    if isinstance(records, tuple) and records[0] is None:
        raise RuntimeError(records[1])
    if not hasattr(records, 'read'):
        write_batches(records, batch_size, output)
        return

    path = os.path.realpath(records.name)
    try:
        pull = getattr(module, function_name + '_pull')
        while True:
            batch = pull(records, batch_size)
            if isinstance(batch, tuple) and batch[0] in (False, None):
                raise RuntimeError(batch[1])
            if not batch:
                break
            write_batches(batch, batch_size, output)
    finally:
        records.close()
        os.remove(path)


if __name__ == '__main__':
    module, function, artifact, parser, batch_size, worker_pid = sys.argv[1:]
    if hasattr(os, 'setsid'):
        os.setsid()
    if sys.platform.startswith('linux'):
        import ctypes

        # Ask Linux to terminate this process if its Celery worker disappears.
        if ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
            raise RuntimeError("Could not register parser parent-death signal")
        if os.getppid() != int(worker_pid):
            sys.exit(1)
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # Keep plugin print statements and child process output off the record stream.
    fd = os.dup(sys.stdout.fileno())
    if hasattr(os, 'set_inheritable'):
        os.set_inheritable(fd, False)
    elif os.name == 'posix':
        import fcntl
        fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC)
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    with os.fdopen(fd, 'wb') as output:
        try:
            run(module, function, artifact, parser, int(batch_size), output)
        except Exception as exc:
            pickle.dump((False, str(exc)), output, protocol=2)
        else:
            pickle.dump((True, None), output, protocol=2)
