import ast
import json
import os
import subprocess


def auto_interface(file, parser):
    try:
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'werpar.py')
        proc = subprocess.Popen(['python3', script, file], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode:
            lines = stderr.decode('utf-8', 'replace').strip().splitlines()
            detail = lines[-1] if lines else "subprocess exited with status %s" % proc.returncode
            return (None, "%s parser: %s" % (parser, detail))

        records = ast.literal_eval(stdout.decode('utf-8'))
        if not isinstance(records, list):
            raise ValueError("expected a list of records")
        return [record if isinstance(record, dict) else json.loads(record)
                for record in records if record != {}]
    except Exception as exc:
        return (None, "%s parser: %s" % (parser, exc))

