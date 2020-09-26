import os
import subprocess
from os import path


def get_flink_home():
    return os.getenv('FLINK_HOME')


def run_job(args, manual=False, rmi_host=None):
    artifact = path.join(os.getcwd(), 'flink-experiment', 'target', 'flink-experiment-1.0-SNAPSHOT.jar')
    total_args = [path.join(get_flink_home(), 'bin', 'flink'),
                  'run', artifact,
                  '--manual', f'{manual}'] +\
                 args +\
                 (['--rmiHost', f'{rmi_host}'] if manual else [])
    subprocess.run(total_args)
