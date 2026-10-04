"""Read process metrics when /proc is mounted from an outer PID namespace."""
from pathlib import Path
import os, psutil

def current_proc_pid():
    return int(Path('/proc/self/stat').read_text().split()[0])

def metric_pid(local_pid):
    if current_proc_pid()==os.getpid():return local_pid
    if local_pid==os.getpid():return current_proc_pid()
    parent=psutil.Process(current_proc_pid())
    for process in parent.children(recursive=True):
        try:
            line=next(x for x in Path(f'/proc/{process.pid}/status').read_text().splitlines() if x.startswith('NSpid:'))
            if int(line.split()[-1])==local_pid:return process.pid
        except (OSError,StopIteration):continue
    raise RuntimeError(f'Cannot resolve child PID {local_pid} for resource measurement')
