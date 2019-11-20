import os
import subprocess
import time
from os import path

import requests
from requests import ConnectionError


class ValueBarrierExperiment:
    def __init__(self, total_value_nodes, total_values, value_rate, vb_ratio, hb_ratio,
                 out_file='/opt/flink/out/out.txt', stats_file='/opt/flink/out/stats.txt'):
        self.total_value_nodes = total_value_nodes
        self.total_values = total_values
        self.value_rate = value_rate
        self.vb_ratio = vb_ratio
        self.hb_ratio = hb_ratio
        self.out_file = out_file
        self.stats_file = stats_file

    def run(self):
        start_jobmanager()
        total_taskmanagers = self.total_value_nodes + 1
        for i in range(1, total_taskmanagers + 1):
            start_taskmanager(i)
        wait_for_taskmanagers(total_taskmanagers)
        self._run_job()
        for i in range(1, total_taskmanagers + 1):
            stop_taskmanager(i)
        stop_jobmanager()

    def _run_job(self):
        proc = subprocess.run(['docker', 'exec',
                               '-i',
                               f'--user={os.geteuid()}:{os.getegid()}',
                               'job',
                               '/opt/flink/bin/flink',
                               'run',
                               '/job.jar',
                               '--valueNodes', f'{self.total_value_nodes}',
                               '--totalValues', f'{self.total_values}',
                               '--valueRate', f'{self.value_rate:.1f}',
                               '--vbRatio', f'{self.vb_ratio}',
                               '--hbRatio', f'{self.hb_ratio}',
                               '--outputFile', f'{self.out_file}',
                               '--statisticsFile', f'{self.stats_file}'])
        print(proc.args)


def start_jobmanager():
    print('Starting the job manager')
    out_path = path.abspath(path.join(os.getcwd(), 'var', 'out'))
    print(out_path)
    proc = subprocess.run(['docker', 'run',
                           '-di',
                           '--rm',
                           '--network=temp',
                           '--name=job',
                           '--hostname=job',
                           f'--user={os.geteuid()}:{os.getegid()}',
                           '--publish=8081:8081',
                           f'--volume={out_path}:/opt/flink/out',
                           'flinknode',
                           'jobmanager'])
    print(proc.args)

    # Wait until we get a valid response from the job manager's REST API
    time.sleep(2)
    wait_for_taskmanagers(0, 0.1)
    print('Job manager is up')


def stop_jobmanager():
    subprocess.run(['docker', 'stop', 'job'])


def start_taskmanager(task_id):
    print(f'Starting task manager {task_id}')
    out_path = path.abspath(path.join(os.getcwd(), 'var', 'out'))
    print(out_path)
    proc = subprocess.run(['docker', 'run',
                           '-di',
                           '--rm',
                           '--network=temp',
                           f'--name=task{task_id}',
                           f'--hostname=task{task_id}',
                           f'--user={os.geteuid()}:{os.getegid()}',
                           f'--volume={out_path}:/opt/flink/out',
                           'flinknode',
                           'taskmanager',
                           '-Djobmanager.rpc.address=job'])
    print(proc.args)


def stop_taskmanager(task_id):
    subprocess.run(['docker', 'stop', f'task{task_id}'])


def wait_for_taskmanagers(n, sleep_time=0.2):
    while True:
        try:
            response = requests.get('http://localhost:8081/overview').json()
            if 'taskmanagers' in response and response['taskmanagers'] == n:
                break
        except ConnectionError:
            pass
        time.sleep(sleep_time)
