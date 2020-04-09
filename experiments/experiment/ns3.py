import os
import signal
import subprocess
from os import path
from subprocess import PIPE


class NS3Conf:
    def __init__(self, total_time=600, data_rate='1Gbps', delay='5000ns', file_prefix='ns3_log', tracing=False):
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
    return os.environ.get('NS3_HOME', os.getcwd())


def ns3_preprocess(nodes):
    preprocess_script = path.join(os.getcwd(), 'ns3', 'container-pre-ns3.sh')
    for node in nodes:
        subprocess.run([preprocess_script, node])


def start_ns3_process(conf, nodes):
    ns3_preprocess(nodes)
    ns3_home = get_ns3_home()
    waf = path.join(ns3_home, 'waf')
    args = f'scratch/tap-vm {conf.get_args()} --MainNode={nodes[0]} ' + ' '.join(nodes[1:])
    return subprocess.Popen([waf, '--run', args], cwd=ns3_home, restore_signals=False)


def ns3_postprocess(nodes):
    postprocess_script = path.join(os.getcwd(), 'ns3', 'container-post-ns3.sh')
    for node in nodes:
        subprocess.run([postprocess_script, node])


def stop_ns3_process(proc, nodes):
    # We want to send the SIGINT signal, but not to proc itself, since it is
    # the python process managing the NS3 simulation. We actually want to find
    # the process called tap-vm and signal it directly.
    pidof = subprocess.run(['pidof', '-s', 'tap-vm'], stdout=PIPE)
    tapvm = int(pidof.stdout)
    os.kill(tapvm, signal.SIGINT)

    # TODO: subprocess.Popen.wait() is a busy loop; replace with asyncio coroutine
    #       asyncio.create_subprocess_exec()
    proc.wait()
    ns3_postprocess(nodes)


def start_network(node, pid, ip_addr):
    start_network_script = path.join(os.getcwd(), 'ns3', 'container-start-network.sh')
    subprocess.run([start_network_script, node, pid, ip_addr])


def stop_network(node, pid):
    stop_network_script = path.join(os.getcwd(), 'ns3', 'container-stop-network.sh')
    subprocess.run([stop_network_script, node, pid])


def ip_address_generator(n, prefix):
    q, r = divmod(n, 254)
    yield from ('.'.join([prefix, str(x), str(y)])
                for x in range(q + 1)
                for y in range(1, r + 1 if x == q else 255))


def ip_address_map(nodes, prefix='10.12'):
    return dict(zip(nodes, ip_address_generator(len(nodes), prefix)))


def write_hosts(ip_addr_map, hosts_file):
    with open(hosts_file, 'w') as f:
        f.writelines(f'{ip}\t{node}\n' for node, ip in ip_addr_map.items())
