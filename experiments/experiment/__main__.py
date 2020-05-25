import argparse

import results
from suites import suites


def main():
    parser = argparse.ArgumentParser(description='Run Flink experiments')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-s', '--suite', help='Run the given experiment suite')
    group.add_argument('-l', '--list', help='List experiment suites', action='store_true')
    group.add_argument('-f', '--flink-results', help='Process Flink results from the given output directory')
    group.add_argument('-e', '--erlang-results', help='Process Erlang results from the given output directory')
    parser.add_argument('--flink-workers', help='File containing a list of Flink worker hostnames')
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

    if args.suite not in suites:
        parser.print_usage()
        exit(1)

    suites[args.suite].run(args)


if __name__ == '__main__':
    main()
