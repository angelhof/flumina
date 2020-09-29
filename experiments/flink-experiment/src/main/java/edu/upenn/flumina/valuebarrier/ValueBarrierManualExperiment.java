package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.ValueBarrierConfig;
import edu.upenn.flumina.remote.ForkJoinService;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.time.Instant;
import java.util.UUID;

public class ValueBarrierManualExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(ValueBarrierManualExperiment.class);

    private final ValueBarrierConfig conf;

    public ValueBarrierManualExperiment(final ValueBarrierConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);

        // Set up the remote ForkJoin service
        final var valueBarrierService = new ValueBarrierService(conf.getValueNodes());
        @SuppressWarnings("unchecked") final var valueBarrierServiceStub =
                (ForkJoinService<Long, Long>) UnicastRemoteObject.exportObject(valueBarrierService, 0);
        final var valueBarrierServiceName = UUID.randomUUID().toString();
        final var registry = LocateRegistry.getRegistry(conf.getRmiHost());
        registry.rebind(valueBarrierServiceName, valueBarrierServiceStub);

        final var valueSource = new ValueOrHeartbeatSource(conf.getTotalValues(), conf.getValueRate(), startTime);
        final var valueStream = env.addSource(valueSource)
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("values");
        final var barrierSource = new BarrierOrHeartbeatSource(
                conf.getTotalValues(), conf.getValueRate(), conf.getValueBarrierRatio(),
                conf.getHeartbeatRatio(), startTime);
        final var barrierStream = env.addSource(barrierSource)
                .slotSharingGroup("barriers");

        // Broadcast the barrier stream and connect it with the value stream
        // We use a dummy broadcast state descriptor that is never actually used.
        final var broadcastStateDescriptor =
                new MapStateDescriptor<>("BroadcastState", Void.class, Void.class);
        final var broadcastStream = barrierStream.broadcast(broadcastStateDescriptor);

        valueStream.connect(broadcastStream)
                .process(new ValueProcessManual(conf.getRmiHost(), valueBarrierServiceName))
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("values");

        barrierStream
                .flatMap(new FlatMapFunction<BarrierOrHeartbeat, Barrier>() {
                    @Override
                    public void flatMap(final BarrierOrHeartbeat barrierOrHeartbeat, final Collector<Barrier> out) {
                        barrierOrHeartbeat.match(
                                barrier -> {
                                    out.collect(barrier);
                                    return null;
                                },
                                heartbeat -> null
                        );
                    }
                })
                .process(new BarrierProcessManual(conf.getRmiHost(), valueBarrierServiceName))
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE);

        try {
            return env.execute("ValueBarrier Manual Experiment");
        } finally {
            UnicastRemoteObject.unexportObject(valueBarrierService, true);
            try {
                registry.unbind(valueBarrierServiceName);
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
