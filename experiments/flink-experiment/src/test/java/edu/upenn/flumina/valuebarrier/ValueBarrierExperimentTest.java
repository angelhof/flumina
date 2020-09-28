package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;
import edu.upenn.flumina.valuebarrier.data.Value;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

import static edu.upenn.flumina.time.TimeHelper.max;
import static edu.upenn.flumina.time.TimeHelper.min;
import static org.junit.Assert.assertEquals;

public class ValueBarrierExperimentTest {

    private static final Logger LOG = LoggerFactory.getLogger(ValueBarrierExperimentTest.class);

    @ClassRule
    public static final MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(4)
                            .build());

    @Test
    public void testSanity() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromElements(1, 2, 3, 4, 5, 6)
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .reduce(Integer::sum)
                .addSink(new SinkFunction<>() {
                    @Override
                    public void invoke(final Integer value, final Context context) {
                        LOG.info("Cumulative sum: {}", value);
                    }
                }).setParallelism(1);
        env.execute();
    }

    @Test
    public void testValues() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
        env.addSource(new ValueOrHeartbeatSource(10_000, 10.0, Instant.now()))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ValueOrHeartbeat>() {
                    @Override
                    public Watermark checkAndGetNextWatermark(final ValueOrHeartbeat valueOrHeartbeat, final long l) {
                        return new Watermark(l);
                    }

                    @Override
                    public long extractTimestamp(final ValueOrHeartbeat valueOrHeartbeat, final long l) {
                        return valueOrHeartbeat.getLogicalTimestamp();
                    }
                })
                .timeWindowAll(Time.milliseconds(10_001))
                .aggregate(new AggregateFunction<ValueOrHeartbeat, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(final ValueOrHeartbeat valueOrHeartbeat, final Integer acc) {
                        return acc + valueOrHeartbeat.match(val -> 1, hb -> 0);
                    }

                    @Override
                    public Integer getResult(final Integer acc) {
                        return acc;
                    }

                    @Override
                    public Integer merge(final Integer acc1, final Integer acc2) {
                        return acc1 + acc2;
                    }
                })
                .addSink(new SinkFunction<>() {
                    @Override
                    public void invoke(final Integer total, final Context context) {
                        LOG.info("Total values: {}", total);
                        assertEquals(total.intValue(), 40_000);
                    }
                }).setParallelism(1);
        env.execute();
    }

    @Test
    public void testBroadcast() throws Exception {
        // Parameters
        final int totalValues = 10_000;
        final double valueRate = 10.0;
        final int valueNodes = 3;
        final int vbRatio = 1_000;
        final int hbRatio = 10;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(valueNodes + 1);

        final Instant startTime = Instant.now().plusMillis(500L);
        final DataStream<ValueOrHeartbeat> valueStream =
                env.addSource(new ValueOrHeartbeatSource(totalValues, valueRate, startTime)).setParallelism(valueNodes);
        final DataStream<BarrierOrHeartbeat> barrierStream =
                env.addSource(new BarrierOrHeartbeatSource(totalValues, valueRate, vbRatio, hbRatio, startTime)).setParallelism(1);

        final MapStateDescriptor<Void, Void> stateDescriptor =
                new MapStateDescriptor<>("BroadcastState", Void.class, Void.class);
        final BroadcastStream<BarrierOrHeartbeat> broadcastStream = barrierStream.broadcast(stateDescriptor);

        valueStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Tuple2<Long, Long>>() {
                    private Instant valueTimestamp = Instant.MIN;
                    private Instant barrierTimestamp = Instant.MIN;
                    private long sum = 0;

                    private final Deque<Value> unprocessedValues = new ArrayDeque<>();
                    private final Deque<Barrier> unprocessedBarriers = new ArrayDeque<>();

                    @Override
                    public void processElement(final ValueOrHeartbeat item,
                                               final ReadOnlyContext ctx,
                                               final Collector<Tuple2<Long, Long>> collector) {
                        item.<Void>match(
                                value -> {
                                    unprocessedValues.addLast(value);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        valueTimestamp = max(valueTimestamp, item.getPhysicalTimestamp());
                        makeProgress(collector);
                    }

                    @Override
                    public void processBroadcastElement(final BarrierOrHeartbeat item,
                                                        final Context ctx,
                                                        final Collector<Tuple2<Long, Long>> collector) {
                        item.<Void>match(
                                barrier -> {
                                    unprocessedBarriers.addLast(barrier);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        barrierTimestamp = max(barrierTimestamp, item.getPhysicalTimestamp());
                        makeProgress(collector);
                    }

                    private void makeProgress(final Collector<Tuple2<Long, Long>> collector) {
                        final Instant currentTime = min(valueTimestamp, barrierTimestamp);
                        while (!unprocessedValues.isEmpty() &&
                                unprocessedValues.getFirst().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
                            final Value value = unprocessedValues.removeFirst();
                            while (!unprocessedBarriers.isEmpty() &&
                                    unprocessedBarriers.getFirst().getPhysicalTimestamp().isBefore(value.getPhysicalTimestamp())) {
                                final Barrier barrier = unprocessedBarriers.removeFirst();
                                LOG.debug("[{}] collecting {} @ {}", getRuntimeContext().getIndexOfThisSubtask(), sum, barrier.getLogicalTimestamp());
                                collector.collect(Tuple2.of(sum, barrier.getLogicalTimestamp()));
                                sum = 0;
                            }
                            sum += value.val;
                        }
                        while (!unprocessedBarriers.isEmpty() &&
                                unprocessedBarriers.getFirst().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
                            final Barrier barrier = unprocessedBarriers.removeFirst();
                            LOG.debug("[{}] collecting {} @ {}", getRuntimeContext().getIndexOfThisSubtask(), sum, barrier.getLogicalTimestamp());
                            collector.collect(Tuple2.of(sum, barrier.getLogicalTimestamp()));
                            sum = 0;
                        }
                    }

                    @Override
                    public void close() {
                        LOG.info("Closing with {} unprocessed values, {} unprocessed barriers, and sum {}", unprocessedValues.size(), unprocessedBarriers.size(), sum);
                    }
                }).setParallelism(valueNodes)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Long, Long>>() {
                    @Override
                    public Watermark checkAndGetNextWatermark(final Tuple2<Long, Long> tuple, final long l) {
                        return new Watermark(l);
                    }

                    @Override
                    public long extractTimestamp(final Tuple2<Long, Long> tuple, final long l) {
                        return tuple.f1;
                    }
                })
                .timeWindowAll(Time.milliseconds(vbRatio))
                .reduce((x, y) -> Tuple2.of(x.f0 + y.f0, x.f1))
                .addSink(new SinkFunction<>() {
                    @Override
                    public void invoke(final Tuple2<Long, Long> out, final Context ctx) {
                        LOG.info("out: {} @ {}", out.f0, out.f1);
                    }
                }).setParallelism(1);
        env.execute();
    }

}
