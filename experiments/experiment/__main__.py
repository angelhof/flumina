import argparse
import os
from os import path

import results
from flink import ValueBarrierExperiment
from ns3 import NS3Conf


class ExperimentSuite:
    def __init__(self, dir_name, experiments):
        self.dir_name = dir_name
        self.experiments = experiments

    def run(self):
        for exp in self.experiments:
            exp.run()
            exp.archive_results(path.join(os.getcwd(), 'archive', self.dir_name))


suites = {
    'exp1': ExperimentSuite(
        'exp1',
        [ValueBarrierExperiment(18, 1_000_000, rate, 1_000, 10, ns3_conf=NS3Conf())
         for rate in range(10, 35, 2)]
    ),
    'exp1-with-5-nodes': ExperimentSuite(
        'exp1-with-5-nodes',
        [ValueBarrierExperiment(5, 1_000_000, rate, 1_000, 10, ns3_conf=NS3Conf())
         for rate in range(20, 51, 2)]
    ),
    'exp1-10Gbps': ExperimentSuite(
        'exp1-10Gbps',
        [ValueBarrierExperiment(18, 1_000_000, rate, 1_000, 10, ns3_conf=NS3Conf(data_rate='10Gbps'))
         for rate in range(10, 35, 2)]
    ),
    'exp2': ExperimentSuite(
        'exp2',
        [ValueBarrierExperiment(n, 1_000_000, 15.0, 1_000, 10, ns3_conf=NS3Conf())
         for n in range(2, 33, 2)]
    ),
    'exp2-10Gbps': ExperimentSuite(
        'exp2-10Gbps',
        [ValueBarrierExperiment(n, 1_000_000, 15.0, 1_000, 10, ns3_conf=NS3Conf(data_rate='10Gbps'))
         for n in range(2, 33, 2)]
    ),
    'exp3': ExperimentSuite(
        'exp3',
        [ValueBarrierExperiment(5, 1_000_000, 15.0, vb_ratio, 10, ns3_conf=NS3Conf())
         for vb_ratio in [30, 40, 50, 100, 200, 500, 1_000]]
    ),
    'exp5': ExperimentSuite(
        'exp5',
        [ValueBarrierExperiment(5, 1_000_000, 15.0, 10_000, hb_ratio, ns3_conf=NS3Conf())
         for hb_ratio in [1, 2, 5, 10, 100, 1_000, 10_000]]
    ),
    'test': ExperimentSuite(
        'test',
        [ValueBarrierExperiment(3, 1_000_000, 100.0, 1_000, 1, ns3_conf=NS3Conf())]
    )
}


def main():
    parser = argparse.ArgumentParser(description='Run Flink experiments')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--suite', help='Run the given experiment suite')
    group.add_argument('-l', '--list', help='List experiment suites', action='store_true')
    group.add_argument('-f', '--flink-results', help='Process Flink results from the given output directory')
    group.add_argument('-e', '--erlang-results', help='Process Erlang results from the given output directory')
    args = parser.parse_args()

    if args.list:
        for name, suite in suites.items():
            print('\n\t'.join(
                [f'{name}:\n' + '=' * (1 + len(name))]
                + [str(exp) for exp in suite.experiments]) + '\n')
        exit(0)

    if args.flink_results is not None:
        p10, p50, p90 = results.get_flink_latencies(args.flink_results)
        throughput = results.get_flink_throughput(args.flink_results)
        network_data = results.get_network_data(args.flink_results) / 1024.0 / 1024.0
        print(f'Latency percentiles (ms):  {p10:.0f}  {p50:.0f}  {p90:.0f}')
        print(f'Mean throughput (events/ms): {throughput}')
        print(f'Network data (MB): {network_data:0.1f}')
        exit(0)

    if args.erlang_results is not None:
        p10, p50, p90 = results.get_erlang_latencies(args.erlang_results)
        throughput = results.get_erlang_throughput(args.erlang_results)
        network_data = results.get_network_data(args.erlang_results) / 1024.0 / 1024.0
        print(f'Latency percentiles (ms):  {p10:.0f}  {p50:.0f}  {p90:.0f}')
        print(f'Mean throughput (events/ms): {throughput}')
        print(f'Network data (MB): {network_data:0.1f}')
        exit(0)

    if args.suite not in suites:
        parser.print_usage()
        exit(1)

    suites[args.suite].run()


if __name__ == '__main__':
    main()
