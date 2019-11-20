import os
import subprocess
from os import path


class NS3Conf:
    def __init__(self, total_time, data_rate='1Gbps', delay='5000ns', file_prefix='ns3_log', tracing=False):
        self.total_time = total_time
        self.data_rate = data_rate
        self.delay = delay
        self.file_prefix = file_prefix
        self.tracing = tracing

    def get_args(self):
        return f'--TotalTime={self.total_time} ' \
               f'--ns3::CsmaChannel::DataRate={self.data_rate} ' \
               f'--ns3::CsmaChannel::Delay={self.delay} ' \
               f'--Tracing={"true" if self.tracing else "false"} ' \
               f'--FilenamePrefix={self.file_prefix} '


def get_ns3_home():
    return os.environ.get('NS3_HOME', '.')


def ns3_preprocess(all_nodes):
    preprocess_script = path.join('ns3', 'container-pre-ns3.sh')
    for node in all_nodes:
        subprocess.run([preprocess_script, node])


def start_ns3_process(conf, main_node, nodes):
    ns3_preprocess([main_node] + nodes)
    ns3_home = get_ns3_home()
    waf = path.join(ns3_home, 'waf')
    args = f'scratch/tap-vm {conf.get_args()} --MainNode={main_node} ' + ' '.join(nodes)
    return subprocess.Popen([waf, '--run', args], cwd=ns3_home)


def ns3_postprocess(all_nodes):
    postprocess_script = path.join('ns3', 'container-post-ns3.sh')
    for node in all_nodes:
        subprocess.run([postprocess_script, node])


def wait_ns3_process(proc, main_node, nodes):
    # TODO: subprocess.Popen.wait() is a busy loop; replace with asyncio coroutine
    #       asyncio.create_subprocess_exec()
    proc.wait()
    ns3_postprocess([main_node] + nodes)


def start_network(node, pid, ip_addr):
    start_network_script = path.join('ns3', 'container-start-network.sh')
    subprocess.run([start_network_script, node, pid, ip_addr])


def stop_network(node, pid):
    stop_network_script = path.join('ns3', 'container-stop-network.sh')
    subprocess.run([stop_network_script, node, pid])
