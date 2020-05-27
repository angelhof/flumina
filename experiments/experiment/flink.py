import os
import shutil
import subprocess
import time
from os import path

import docker
import ns3
from ec2 import run_job


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
        self.nodes = ['job'] + [f'task{n:02}' for n in range(1, total_value_nodes + 2)]

    def __str__(self):
        return 'ValueBarrierExperiment(' \
               f'value_nodes={self.total_value_nodes}, ' \
               f'values={self.total_values}, ' \
               f'value_rate={self.value_rate:.1f}, ' \
               f'vb_ratio={self.vb_ratio}, ' \
               f'hb_ratio={self.hb_ratio}, ' \
               f'ns3={self._with_ns3()})'

    def _with_ns3(self):
        return self.ns3_conf is not None

    def run(self, args):
        self._prepare()

        if self._with_ns3():
            # self.ns3_conf.total_time = self._approx_ns3_time()
            self.ns3_proc = ns3.start_ns3_process(self.ns3_conf, self.nodes)

            # Give it a couple of seconds to start
            # TODO: Better synchronization
            time.sleep(3)

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

        self.log_path = path.abspath(path.join(os.getcwd(), 'var', 'log'))
        for node in self.nodes:
            os.makedirs(path.join(self.log_path, node), exist_ok=True)

        self.notify = path.join(self.conf_path, 'notify')
        if not path.exists(self.notify):
            os.mkfifo(self.notify)

        if self.ns3_conf is not None:
            self.ip_addr_map = ns3.ip_address_map(self.nodes)
            ns3.write_hosts(self.ip_addr_map, path.join(self.conf_path, 'hosts'))
            self.pid_map = {}

    def _wait_on_pipe(self):
        with open(self.notify, 'r') as p:
            result = p.read()
            print(f'Notification pipe: {result}')

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
        args = ['/usr/bin/docker', 'run',
                '-d',
                '--rm',
                f'--network={network}',
                f'--name={node}',
                f'--hostname={node}',
                f'--volume={self.out_path}:/opt/flink/out',
                f'--volume={self.conf_path}:/conf',
                f'--volume={path.join(self.log_path, node)}:/log',
                'flumina-flink',
                command] \
               + (['--with-ns3'] if self._with_ns3() else []) \
               + extra_args
        subprocess.run(args)
        if self._with_ns3():
            pid = docker.inspect_pid(node)
            self.pid_map[node] = pid
            ns3.start_network(node, pid, self.ip_addr_map[node])

    def _run_job(self):
        subprocess.run(['/usr/bin/docker', 'exec',
                        self.nodes[0],
                        '/opt/flink/bin/flink',
                        'run',
                        '/job.jar',
                        '--experiment', 'value-barrier'
                        '--valueNodes', f'{self.total_value_nodes}',
                        '--totalValues', f'{self.total_values}',
                        '--valueRate', f'{self.value_rate:.1f}',
                        '--vbRatio', f'{self.vb_ratio}',
                        '--hbRatio', f'{self.hb_ratio}',
                        '--outFile', self.out_file,
                        '--statsFile', self.stats_file])

    def _stop_nodes(self):
        if self._with_ns3():
            for node in self.nodes:
                ns3.stop_network(node, self.pid_map[node])
        subprocess.run(['/usr/bin/docker', 'stop'] + self.nodes)

    def _cleanup(self):
        os.unlink(self.notify)

    def archive_results(self, to_path):
        exp_dir_name = f'n{self.total_value_nodes}_r{self.value_rate:.0f}_q{self.vb_ratio}_h{self.hb_ratio}'
        exp_path = path.join(to_path, exp_dir_name)
        if path.isdir(exp_path):
            shutil.rmtree(exp_path)
        shutil.move(self.out_path, exp_path)
        for node in self.nodes:
            shutil.move(path.join(self.log_path, node), exp_path)
        if self._with_ns3():
            ns3_home = ns3.get_ns3_home()
            files = [
                path.join(ns3_home, f)
                for f in os.listdir(ns3_home)
                if f.startswith(self.ns3_conf.file_prefix)
            ]
            for f in files:
                shutil.move(f, exp_path)


class ValueBarrierEC2:
    def __init__(self, total_value_nodes, total_values, value_rate, vb_ratio, hb_ratio,
                 out_file='out.txt', stats_file='stats.txt'):
        self.total_value_nodes = total_value_nodes
        self.total_values = total_values
        self.value_rate = value_rate
        self.vb_ratio = vb_ratio
        self.hb_ratio = hb_ratio
        self.out_file = out_file
        self.stats_file = stats_file

    def __str__(self):
        return 'ValueBarrierEC2(' \
               f'value_nodes={self.total_value_nodes}, ' \
               f'values={self.total_values}, ' \
               f'value_rate={self.value_rate:.1f}, ' \
               f'vb_ratio={self.vb_ratio}, ' \
               f'hb_ratio={self.hb_ratio})'

    def run(self, args):
        if not args.flink_workers:
            print('A file containing a list of Flink worker hostnames hasn\'t been provided')
            exit(1)
        self.flink_workers = args.flink_workers
        run_job(['--experiment', 'value-barrier',
                 '--valueNodes', f'{self.total_value_nodes}',
                 '--totalValues', f'{self.total_values}',
                 '--valueRate', f'{self.value_rate:.1f}',
                 '--vbRatio', f'{self.vb_ratio}',
                 '--hbRatio', f'{self.hb_ratio}'])

    def archive_results(self, to_path):
        exp_dir_name = f'n{self.total_value_nodes}_r{self.value_rate:.0f}_q{self.vb_ratio}_h{self.hb_ratio}'
        exp_path = path.join(to_path, exp_dir_name)
        if path.isdir(exp_path):
            shutil.rmtree(exp_path)
        os.makedirs(exp_path)

        with open(self.flink_workers, 'r') as f:
            for host in f:
                # Here we assume that the Flink cluster was started from the home directory,
                # and self.out_file is given relative to home.
                subprocess.run(['scp',
                                host.rstrip() + ':' + self.out_file,
                                exp_path])
                subprocess.run(['ssh', host.rstrip(), 'rm', self.out_file])
        shutil.move(self.stats_file, exp_path)


class PageViewEC2:
    def __init__(self, total_pageviews, total_users, pageview_parallelism, pageview_rate,
                 out_file='out.txt', stats_file='stats.txt'):
        self.total_pageviews = total_pageviews
        self.total_users = total_users
        self.pageview_parallelism = pageview_parallelism
        self.pageview_rate = pageview_rate
        self.out_file = out_file
        self.stats_file = stats_file

    def __str__(self):
        return 'PageViewEC2(' \
            f'total_pageviews={self.total_pageviews}, ' \
            f'total_users={self.total_users}, ' \
            f'pageview_parallelism={self.pageview_parallelism}, ' \
            f'pageview_rate={self.pageview_rate:.1f})'

    def run(self, args):
        if not args.flink_workers:
            print('A file containing a list of Flink worker hostnames hasn\'t been provided')
            exit(1)
        self.flink_workers = args.flink_workers
        run_job(['--experiment', 'pageview',
                 '--totalPageViews', f'{self.total_pageviews}',
                 '--totalUsers', f'{self.total_users}',
                 '--pageViewParallelism', f'{self.pageview_parallelism}',
                 '--pageViewRate', f'{self.pageview_rate:.1f}'])

    def archive_results(self, to_path):
        exp_dir_name = f'u{self.total_users}_p{self.pageview_parallelism}_r{self.pageview_rate:.0f}'
        exp_path = path.join(to_path, exp_dir_name)
        if path.isdir(exp_path):
            shutil.rmtree(exp_path)
        os.makedirs(exp_path)

        with open(self.flink_workers, 'r') as f:
            for host in f:
                # Here we assume that the Flink cluster was started from the home directory,
                # and self.out_file is given relative to home.
                subprocess.run(['scp',
                                host.rstrip() + ':' + self.out_file,
                                exp_path])
                subprocess.run(['ssh', host.rstrip(), 'rm', self.out_file])
        shutil.move(self.stats_file, exp_path)


class FraudDetectionEC2:
    def __init__(self, total_value_nodes, total_values, value_rate, vb_ratio, hb_ratio,
                 out_file='out.txt', stats_file='stats.txt'):
        self.total_value_nodes = total_value_nodes
        self.total_values = total_values
        self.value_rate = value_rate
        self.vb_ratio = vb_ratio
        self.hb_ratio = hb_ratio
        self.out_file = out_file
        self.stats_file = stats_file

    def __str__(self):
        return 'FraudDetectionEC2(' \
               f'value_nodes={self.total_value_nodes}, ' \
               f'values={self.total_values}, ' \
               f'value_rate={self.value_rate:.1f}, ' \
               f'vb_ratio={self.vb_ratio}, ' \
               f'hb_ratio={self.hb_ratio})'

    def run(self, args):
        if not args.flink_workers:
            print('A file containing a list of Flink worker hostnames hasn\'t been provided')
            exit(1)
        self.flink_workers = args.flink_workers
        run_job(['--experiment', 'fraud-detection',
                 '--valueNodes', f'{self.total_value_nodes}',
                 '--totalValues', f'{self.total_values}',
                 '--valueRate', f'{self.value_rate:.1f}',
                 '--vbRatio', f'{self.vb_ratio}',
                 '--hbRatio', f'{self.hb_ratio}'])

    def archive_results(self, to_path):
        exp_dir_name = f'n{self.total_value_nodes}_r{self.value_rate:.0f}_q{self.vb_ratio}_h{self.hb_ratio}'
        exp_path = path.join(to_path, exp_dir_name)
        if path.isdir(exp_path):
            shutil.rmtree(exp_path)
        os.makedirs(exp_path)

        with open(self.flink_workers, 'r') as f:
            for host in f:
                # Here we assume that the Flink cluster was started from the home directory,
                # and self.out_file is given relative to home.
                subprocess.run(['scp',
                                host.rstrip() + ':' + self.out_file,
                                exp_path])
                subprocess.run(['ssh', host.rstrip(), 'rm', self.out_file])
        shutil.move(self.stats_file, exp_path)
