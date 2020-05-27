import argparse

import results
from flink import ValueBarrierEC2, PageViewEC2, FraudDetectionEC2
from suites import suites, ExperimentSuite


def main():
    parser = argparse.ArgumentParser(description='Run Flink experiments')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--suite', help='Run the given experiment suite')
    group.add_argument('-e', '--experiment', help='Run the given experiment')
    group.add_argument('-l', '--list', help='List experiment suites', action='store_true')
    group.add_argument('-k', '--flink-results', help='Process Flink results from the given output directory')
    group.add_argument('-f', '--flumina-results', help='Process Flumina results from the given output directory')
    parser.add_argument('--flink-workers', help='File containing a list of Flink worker hostnames')
    parser.add_argument('--total-values', type=int)
    parser.add_argument('--value-nodes', type=int)
    parser.add_argument('--value-rate', type=float)
    parser.add_argument('--vb-ratio', type=int)
    parser.add_argument('--hb-ratio', type=int)
    parser.add_argument('--total-pageviews', type=int)
    parser.add_argument('--total-users', type=int, default=2)
    parser.add_argument('--pageview-parallelism', type=int)
    parser.add_argument('--pageview-rate', type=float)
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
        #network_data = results.get_network_data(args.flink_results) / 1024.0 / 1024.0
        print(f'Latency percentiles (ms):  {p10:.0f}  {p50:.0f}  {p90:.0f}')
        print(f'Mean throughput (events/ms): {throughput}')
        #print(f'Network data (MB): {network_data:0.1f}')
        exit(0)

    if args.erlang_results is not None:
        p10, p50, p90 = results.get_erlang_latencies(args.erlang_results)
        throughput = results.get_erlang_throughput(args.erlang_results)
        network_data = results.get_network_data(args.erlang_results) / 1024.0 / 1024.0
        print(f'Latency percentiles (ms):  {p10:.0f}  {p50:.0f}  {p90:.0f}')
        print(f'Mean throughput (events/ms): {throughput}')
        print(f'Network data (MB): {network_data:0.1f}')
        exit(0)

    if args.experiment is not None:
        if args.experiment.startswith("value-barrier"):
            exp = ValueBarrierEC2(args.value_nodes, args.total_values, args.value_rate, args.vb_ratio, args.hb_ratio)
        elif args.experiment.startswith("pageview"):
            exp = PageViewEC2(args.total_pageviews, args.total_users, args.pageview_parallelism, args.pageview_rate)
        elif args.experiment.startswith("fraud-detection"):
            exp = FraudDetectionEC2(args.value_nodes, args.total_values, args.value_rate, args.vb_ratio, args.hb_ratio)
        ExperimentSuite(args.experiment, [exp]).run(args)
        exit(0)

    if args.suite not in suites:
        parser.print_usage()
        exit(1)

    suites[args.suite].run(args)


if __name__ == '__main__':
    main()
