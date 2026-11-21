import os
import time


def parse(file, parser):
    if parser == "hang":
        time.sleep(30)
    if parser == "crash":
        raise ValueError("fixture crashed")
    if parser == "stream":
        return open(file)
    return [{"artifact": file}]


def parse_pull(file, batch_size):
    return [{"line": line.strip()} for line in file.readlines(batch_size)]


def generate(file, parser):
    print("parser diagnostic")
    yield {"record": 1}
    if parser == "fail-after-first":
        raise ValueError("failed after first batch")
    while not os.path.exists(file):
        time.sleep(0.05)
    yield {"record": 2}
