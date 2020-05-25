import os
import subprocess
from os import path


def get_flink_home():
    return os.getenv('FLINK_HOME')


def run_job(args):
    artifact = path.join(os.getcwd(), 'flink-experiment', 'target', 'flink-experiment-1.0-SNAPSHOT.jar')
    total_args = [path.join(get_flink_home(), 'bin', 'flink'),
                  'run',
                  artifact] \
                 + args
    subprocess.run(total_args)
