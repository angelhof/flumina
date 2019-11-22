import subprocess


def inspect_pid(container):
    proc = subprocess.run(['/usr/bin/docker', 'inspect',
                           '--format', '{{ .State.Pid }}',
                           container],
                          capture_output=True)
    return proc.stdout.decode('utf-8').rstrip()
