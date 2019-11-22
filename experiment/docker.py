import subprocess
from subprocess import PIPE


def inspect_pid(container):
    proc = subprocess.run(['/usr/bin/docker', 'inspect',
                           '--format', '{{ .State.Pid }}',
                           container],
                          stdout=PIPE)
    return proc.stdout.decode('utf-8').rstrip()
