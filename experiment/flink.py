import os
import subprocess
import time
from os import path

import docker
import ns3


class ValueBarrierExperiment:
    def __init__(self, total_value_nodes, total_values, value_rate, vb_ratio, hb_ratio,
                 out_file='out.txt', stats_file='stats.txt', ns3_conf=None):
        self.total_value_nodes = total_value_nodes
        self.total_values = total_values
        self.value_rate = value_rate
        self.vb_ratio = vb_ratio
        self.hb_ratio = hb_ratio
        self.out_file = path.join('/opt/flink/out', out_file)
        self.stats_file = path.join('/opt/flink/out', stats_file)
        self.ns3_conf = ns3_conf
        self.ns3_proc = None
        self.nodes = ['job'] + [f'{n:02}' for n in range(1, total_value_nodes + 2)]

    def _with_ns3(self):
        return self.ns3_conf is not None

    def run(self):
        self._prepare()

        if self._with_ns3():
            self.ns3_proc = ns3.start_ns3_process(self.ns3_conf, self.nodes[0], self.nodes[1:])

            # Give it a couple of seconds to start
            # TODO: Better synchronization
            time.sleep(2)

        self._start_jobmanager()
        self._start_taskmanagers()
        self._run_job()
        self._stop_nodes()

        if self._with_ns3():
            ns3.stop_ns3_process(self.ns3_proc, self.nodes)

        self._cleanup()

    def _prepare(self):
        self.out_path = path.abspath(path.join(os.getcwd(), 'var', 'out'))
        self.conf_path = path.abspath(path.join(os.getcwd(), 'var', 'conf'))
        os.makedirs(self.out_path, exist_ok=True)
        os.makedirs(self.conf_path, exist_ok=True)

        self.notify = path.join(self.conf_path, 'notify')
        if not path.exists(self.notify):
            os.mkfifo(self.notify)

        if self.ns3_conf is not None:
            self.ip_addr_map = ns3.ip_address_map(self.nodes)
            ns3.write_hosts(self.ip_addr_map, path.join(self.conf_path, 'hosts'))
            self.pid_map = {}

    def _wait_on_pipe(self):
        with open(self.notify, 'r') as p:
            p.read()

    def _start_jobmanager(self):
        self._start_node(self.nodes[0], 'jobmanager', ['--wait-taskmanagers', f'{self.total_value_nodes + 1}'])
        self._wait_on_pipe()

    def _start_taskmanagers(self):
        for task_node in self.nodes[1:]:
            self._start_taskmanager(task_node)
        self._wait_on_pipe()

    def _start_taskmanager(self, node):
        self._start_node(node, 'taskmanager', [f'-Djobmanager.rpc.address={self.nodes[0]}'])

    def _start_node(self, node, command, extra_args):
        network = 'none' if self._with_ns3() else 'temp'
        args = ['docker', 'run',
                '-di',
                '--rm',
                f'--network={network}',
                f'--name={node}',
                f'--hostname={node}',
                f'--volume={self.out_path}:/opt/flink/out',
                f'--volume={self.conf_path}:/conf',
                'flinknode',
                command] \
               + (['--with-ns3'] if self._with_ns3() else []) \
               + extra_args
        subprocess.run(args)
        if self._with_ns3():
            pid = docker.inspect_pid(node)
            self.pid_map[node] = pid
            ns3.start_network(node, pid, self.ip_addr_map[node])

    def _run_job(self):
        subprocess.run(['docker', 'exec',
                        '-i',
                        self.nodes[0],
                        '/opt/flink/bin/flink',
                        'run',
                        '/job.jar',
                        '--valueNodes', f'{self.total_value_nodes}',
                        '--totalValues', f'{self.total_values}',
                        '--valueRate', f'{self.value_rate:.1f}',
                        '--vbRatio', f'{self.vb_ratio}',
                        '--hbRatio', f'{self.hb_ratio}',
                        '--outputFile', self.out_file,
                        '--statisticsFile', self.stats_file])

    def _stop_nodes(self):
        if self._with_ns3():
            for node in self.nodes:
                ns3.stop_network(node, self.pid_map[node])
        subprocess.run(['docker', 'stop'] + self.nodes)

    def _cleanup(self):
        os.unlink(self.notify)
