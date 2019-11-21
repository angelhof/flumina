import subprocess


def inspect_pid(container):
    proc = subprocess.run(['docker', 'inspect',
                           '--format', '{{ .State.Pid }}',
                           container],
                          capture_output=True)
    return int(proc.stdout)
