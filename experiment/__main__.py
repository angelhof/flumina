from flink import ValueBarrierExperiment


def main():
    experiment = ValueBarrierExperiment(3, 1_000_000, 100.0, 1_000, 1)
    experiment.run()


if __name__ == '__main__':
    main()
