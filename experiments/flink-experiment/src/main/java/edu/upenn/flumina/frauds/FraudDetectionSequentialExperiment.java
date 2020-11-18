package edu.upenn.flumina.frauds;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.FraudDetectionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;

public class FraudDetectionSequentialExperiment implements Experiment {

    private final FraudDetectionConfig conf;

    public FraudDetectionSequentialExperiment(final FraudDetectionConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);

        final var transactionSource = new TransactionOrHeartbeatSource(
                conf.getTotalValues(), conf.getValueRate(), startTime);
        final var transactionStream = env.addSource(transactionSource)
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("transactions");
        final var ruleSource = new RuleOrHeartbeatSource(
                conf.getTotalValues(), conf.getValueRate(), conf.getValueBarrierRatio(),
                conf.getHeartbeatRatio(), startTime);
        final var ruleStream = env.addSource(ruleSource)
                .slotSharingGroup("rules");

        ruleStream.connect(transactionStream)
                .process(new FraudProcessFunction(conf.getValueNodes()))
                .slotSharingGroup("rules")
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE);

        return env.execute("FraudDetection Experiment");
    }

    @Override
    public long getTotalEvents() {
        return conf.getValueNodes() * conf.getTotalValues() + conf.getTotalValues() / conf.getValueBarrierRatio();
    }

    @Override
    public long getOptimalThroughput() {
        return (long) (conf.getValueRate() * conf.getValueNodes() + conf.getValueRate() / conf.getValueBarrierRatio());
    }

}
