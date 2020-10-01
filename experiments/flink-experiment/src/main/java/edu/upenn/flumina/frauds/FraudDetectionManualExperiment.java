package edu.upenn.flumina.frauds;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.FraudDetectionConfig;
import edu.upenn.flumina.frauds.data.Rule;
import edu.upenn.flumina.frauds.data.RuleOrHeartbeat;
import edu.upenn.flumina.remote.ForkJoinService;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.time.Instant;
import java.util.UUID;

public class FraudDetectionManualExperiment implements Experiment {

    private final FraudDetectionConfig conf;

    public FraudDetectionManualExperiment(final FraudDetectionConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);

        // Set up the remote ForkJoin service
        final var fraudDetectionService = new FraudDetectionService(conf.getValueNodes());
        @SuppressWarnings("unchecked") final var fraudDetectionServiceStub =
                (ForkJoinService<Tuple2<Long, Long>, Tuple2<Long, Long>>) UnicastRemoteObject.exportObject(fraudDetectionService, 0);
        final var fraudDetectionServiceName = UUID.randomUUID().toString();
        final var registry = LocateRegistry.getRegistry(conf.getRmiHost());
        registry.rebind(fraudDetectionServiceName, fraudDetectionServiceStub);

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

        final var broadcastStateDescriptor = new MapStateDescriptor<>("BroadcastState", Void.class, Void.class);
        final var broadcastRuleStream = ruleStream.broadcast(broadcastStateDescriptor);

        transactionStream.connect(broadcastRuleStream)
                .process(new TransactionProcessManual(conf.getRmiHost(), fraudDetectionServiceName))
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("transactions")
                .map(new TimestampMapper())
                .setParallelism(conf.getValueNodes())
                .writeAsText(conf.getTransOutFile(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(conf.getValueNodes());

        ruleStream
                .flatMap(new FlatMapFunction<RuleOrHeartbeat, Rule>() {
                    @Override
                    public void flatMap(final RuleOrHeartbeat ruleOrHeartbeat, final Collector<Rule> out) {
                        ruleOrHeartbeat.match(
                                rule -> {
                                    out.collect(rule);
                                    return null;
                                },
                                heartbeat -> null
                        );
                    }
                })
                .process(new RuleProcessManual(conf.getRmiHost(), fraudDetectionServiceName))
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE);

        try {
            return env.execute("FraudDetection Manual Experiment");
        } finally {
            UnicastRemoteObject.unexportObject(fraudDetectionService, true);
            try {
                registry.unbind(fraudDetectionServiceName);
            } catch (final NotBoundException ignored) {

            }
        }
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
