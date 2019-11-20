class NS3Conf:
    def __init__(self, total_time, data_rate='1Gbps', delay='5000ns', tracing=False):
        self.total_time = total_time
        self.data_rate = data_rate
        self.delay = delay
        self.tracing = tracing

    def generate_args(self, file_prefix):
        args = ("--TotalTime={} "
                "--ns3::CsmaChannel::DataRate={} "
                "--ns3::CsmaChannel::Delay={} "
                "--Tracing={} "
                "--FilenamePrefix={} ")
        return args.format(self.total_time,
                           self.data_rate,
                           self.delay,
                           'true' if self.tracing else 'false',
                           file_prefix)


def setup_container(name, container_id, pid):
    pass
