import os
from os import path

from flink import ValueBarrierExperiment, ValueBarrierEC2, PageViewEC2
from ns3 import NS3Conf


class ExperimentSuite:
    def __init__(self, dir_name, experiments):
        self.dir_name = dir_name
        self.experiments = experiments

    def run(self, args):
        for exp in self.experiments:
            exp.run(args)
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
         for rate in range(20, 101, 2)]
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
        [ValueBarrierExperiment(3, 1_000_000, 100.0, 1_000, 10, ns3_conf=NS3Conf())]
    ),
    'test-big': ExperimentSuite(
        'test-big',
        [ValueBarrierExperiment(10, 1_000_000, 50.0, 1_000, 10, ns3_conf=NS3Conf())]
    ),
    'value-barrier-test-ec2': ExperimentSuite(
        'value-barrier-test-ec2',
        [ValueBarrierEC2(2, 1_000_000, 20, 1_000, 10)]
    ),
    'value-barrier-rates-ec2': ExperimentSuite(
        'value-barrier-rates-ec2',
        [ValueBarrierEC2(5, 1_000_000, r, 1_000, 10) for r in range(20, 101, 2)]
    ),
    'value-barrier-nodes-ec2': ExperimentSuite(
        'value-barrier-nodes-ec2',
        [ValueBarrierEC2(n, 1_000_000, 20, 1_000, 10) for n in range(2, 39, 2)]
    ),
    'pageview-test-ec2': ExperimentSuite(
        'pageview-test-ec2',
        [PageViewEC2(500_000, 2, 2, 20)]
    ),
    'pageview-rates-ec2': ExperimentSuite(
        'pageview-rates-ec2',
        [PageViewEC2(500_000, 2, 2, r) for r in range(2, 101, 2)]
    ),
    'pageview-parallelism-ec2': ExperimentSuite(
        'pageview-parallelism-ec2',
        [PageViewEC2(500_000, 2, p, 15) for p in range(2, 41, 2)]
    )
}
