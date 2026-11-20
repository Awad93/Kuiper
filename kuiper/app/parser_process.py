"""Run one parser in a separate process and spool its record batches."""

import importlib
import os
import pickle
import signal
import sys


def run(module_name, function_name, artifact, parser_name, batch_size, output):
    module = importlib.import_module(module_name)
    records = getattr(module, function_name)(file=artifact, parser=parser_name)
    if isinstance(records, tuple) and records[0] is None:
        raise RuntimeError(records[1])
    if not hasattr(records, 'read'):
        pickle.dump(records, output, protocol=2)
        return

    path = os.path.realpath(records.name)
    try:
        pull = getattr(module, function_name + '_pull')
        while True:
            batch = pull(records, batch_size)
            if isinstance(batch, tuple) and batch[0] is False:
                raise RuntimeError(batch[1])
            if not batch:
                break
            pickle.dump(batch, output, protocol=2)
    finally:
        records.close()
        os.remove(path)


if __name__ == '__main__':
    module, function, artifact, parser, batch_size, output_path, worker_pid = sys.argv[1:]
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
    with open(output_path, 'wb') as output:
        try:
            run(module, function, artifact, parser, int(batch_size), output)
        except Exception as exc:
            # Start the result again so partial parser output is never ingested.
            output.seek(0)
            output.truncate()
            pickle.dump((False, str(exc)), output, protocol=2)
