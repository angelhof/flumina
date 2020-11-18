package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.ValueBarrierConfig;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;
import edu.upenn.flumina.valuebarrier.data.Value;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static edu.upenn.flumina.time.TimeHelper.min;

public class ValueBarrierExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(ValueBarrierExperiment.class);

    private final ValueBarrierConfig conf;

    public ValueBarrierExperiment(final ValueBarrierConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
                .process(new BroadcastProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Tuple3<Long, Long, Instant>>() {
                    private Instant valuePhysicalTimestamp = Instant.MIN;
                    private Instant barrierPhysicalTimestamp = Instant.MIN;
                    private long sum = 0;

                    private final Queue<Value> unprocessedValues = new ArrayDeque<>();
                    private final Queue<Barrier> unprocessedBarriers = new ArrayDeque<>();

                    @Override
                    public void processElement(final ValueOrHeartbeat valueOrHeartbeat,
                                               final ReadOnlyContext ctx,
                                               final Collector<Tuple3<Long, Long, Instant>> collector) {
                        unprocessedValues.addAll(valueOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
                        valuePhysicalTimestamp = valueOrHeartbeat.getPhysicalTimestamp();
                        makeProgress(collector);
                    }

                    @Override
                    public void processBroadcastElement(final BarrierOrHeartbeat barrierOrHeartbeat,
                                                        final Context ctx,
                                                        final Collector<Tuple3<Long, Long, Instant>> collector) {
                        unprocessedBarriers.addAll(barrierOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
                        barrierPhysicalTimestamp = barrierOrHeartbeat.getPhysicalTimestamp();
                        makeProgress(collector);
                    }

                    private void makeProgress(final Collector<Tuple3<Long, Long, Instant>> collector) {
                        final var currentTime = min(valuePhysicalTimestamp, barrierPhysicalTimestamp);
                        while (!unprocessedValues.isEmpty() &&
                                unprocessedValues.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
                            final var value = unprocessedValues.remove();
                            while (!unprocessedBarriers.isEmpty() &&
                                    unprocessedBarriers.element().getPhysicalTimestamp().isBefore(value.getPhysicalTimestamp())) {
                                update(unprocessedBarriers.remove(), collector);
                            }
                            update(value, collector);
                        }
                        while (!unprocessedBarriers.isEmpty() &&
                                unprocessedBarriers.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
                            update(unprocessedBarriers.remove(), collector);
                        }
                    }

                    private void update(final Value value, final Collector<Tuple3<Long, Long, Instant>> collector) {
                        sum += value.val;
                    }

                    private void update(final Barrier barrier, final Collector<Tuple3<Long, Long, Instant>> collector) {
                        collector.collect(Tuple3.of(sum, barrier.getLogicalTimestamp(), barrier.getPhysicalTimestamp()));
                        sum = 0;
                    }
                })
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("values")
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<Long, Long, Instant>>() {
                    @Override
                    public Watermark checkAndGetNextWatermark(final Tuple3<Long, Long, Instant> tuple, final long l) {
                        return new Watermark(l);
                    }

                    @Override
                    public long extractTimestamp(final Tuple3<Long, Long, Instant> tuple, final long l) {
                        // The field tuple.f1 corresponds to the logical timestamp
                        return tuple.f1;
                    }
                })
                .setParallelism(conf.getValueNodes())
                .timeWindowAll(Time.milliseconds(conf.getValueBarrierRatio()))
                .reduce((x, y) -> {
                    x.f0 += y.f0;
                    return x;
                })
                .slotSharingGroup("barriers")
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE);

        return env.execute("ValueBarrier Experiment");
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
