import os
from itertools import chain
from os import path

from flink import ValueBarrierExperiment, ValueBarrierEC2, PageViewEC2, FraudDetectionEC2
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

    # Old NS3 suites

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

    # EC2 suites

    # Value-Barrier

    'value-barrier-test': ExperimentSuite(
        'value-barrier-test',
        [ValueBarrierEC2(3, 300 * 10_000, 300, 1000, 10, attempt=a, sequential=True) for a in [1, 2, 3]]
    ),
    'value-barrier-sequential': ExperimentSuite(
        'value-barrier-sequential',
        # By making the number of values 30_000 times the rate, we aim at the experiment ideally taking 30s
        [ValueBarrierEC2(1, r * 30_000, r, 10_000, 100, sequential=True, attempt=a)
         for r in range(400, 441, 20)
         for a in [1, 2, 3]]
    ),
    'value-barrier': ExperimentSuite(
        'value-barrier',
        [ValueBarrierEC2(p, r * 30_000, r, 10_000, 100, attempt=a)
         for p in chain([1], range(2, 21, 2))
         for r in range(360, 441, 10)
         for a in [1]]
    ),
    'value-barrier-manual': ExperimentSuite(
        'value-barrier-manual',
        [ValueBarrierEC2(p, r * 30_000, r, 10_000, 100, attempt=a, manual=True)
         for p in chain([1], range(2, 21, 2))
         for r in range(360, 441, 10)
         for a in [1]]
    ),

    # PageView

    'pageview-test': ExperimentSuite(
        'pageview-test',
        [PageViewEC2(400 * 10_000, 2, 4, 400, attempt=a) for a in [1, 2, 3]]
    ),
    'pageview-sequential': ExperimentSuite(
        'pageview-sequential',
        [PageViewEC2(r * 30_000, 2, 1, r, sequential=True, attempt=a)
         for r in range(160, 201, 20)
         for a in [1, 2, 3]]
    ),
    'pageview': ExperimentSuite(
        'pageview',
        [PageViewEC2(r * 30_000, 2, p, r, attempt=a)
         for p in chain([1], range(2, 21, 2))
         for r in range(120, 201, 10)
         for a in [1]]
    ),
    'pageview-manual': ExperimentSuite(
        'pageview-manual',
        [PageViewEC2(r * 30_000, 2, p, r, attempt=a, manual=True)
         for p in chain([1], range(2, 21, 2))
         for r in range(120, 201, 10)
         for a in [1]]
    ),

    # Fraud detection

    'fraud-detection-test': ExperimentSuite(
        'fraud-detection-test',
        [FraudDetectionEC2(7, 300 * 10_000, 300, 10_000, 100, attempt=a) for a in [1, 2, 3]]
    ),
    'fraud-detection-sequential': ExperimentSuite(
        'fraud-detection-sequential',
        [FraudDetectionEC2(1, r * 30_000, r, 10_000, 100, attempt=a)
         for r in range(400, 441, 20)
         for a in [1, 2, 3]]
    ),
    'fraud-detection': ExperimentSuite(
        'fraud-detection',
        [FraudDetectionEC2(p, r * 30_000, r, 10_000, 100, attempt=a)
         for p in chain([1], range(2, 21, 2))
         for r in range(360, 441, 10)
         for a in [1]]
    ),
    'fraud-detection-manual': ExperimentSuite(
        'fraud-detection-manual',
        [FraudDetectionEC2(p, r * 30_000, r, 10_000, 100, attempt=a, manual=True)
         for p in chain([1], range(2, 21, 2))
         for r in range(360, 441, 10)
         for a in [1]]
    )
}
