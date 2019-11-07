package edu.upenn.flumina;

import edu.upenn.flumina.data.Barrier;
import edu.upenn.flumina.data.Value;
import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;
import edu.upenn.flumina.source.BarrierSource;
import edu.upenn.flumina.source.ValueSource;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.Assert.assertEquals;

public class ValueBarrierExperimentTest {

    private static final Logger LOG = LogManager.getLogger();

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(4)
                            .build());

    @Test
    public void testSanity() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromElements(1, 2, 3, 4, 5, 6)
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .reduce((x, y) -> x + y)
                .addSink(new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value, Context context) {
                        LOG.info("Cumulative sum: {}", value);
                    }
                }).setParallelism(1);
        env.execute();
    }

    @Test
    public void testValues() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
        env.addSource(new ValueSource(10_000, 10.0))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ValueOrHeartbeat>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(ValueOrHeartbeat valueOrHeartbeat, long l) {
                        return new Watermark(l);
                    }

                    @Override
                    public long extractTimestamp(ValueOrHeartbeat valueOrHeartbeat, long l) {
                        return valueOrHeartbeat.getTimestamp();
                    }
                })
                .timeWindowAll(Time.milliseconds(10_001))
                .aggregate(new AggregateFunction<ValueOrHeartbeat, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(ValueOrHeartbeat valueOrHeartbeat, Integer acc) {
                        return acc + valueOrHeartbeat.match(val -> 1, hb -> 0);
                    }

                    @Override
                    public Integer getResult(Integer acc) {
                        return acc;
                    }

                    @Override
                    public Integer merge(Integer acc1, Integer acc2) {
                        return acc1 + acc2;
                    }
                })
                .addSink(new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer total, Context context) {
                        LOG.info("Total values: {}", total);
                        assertEquals(total.intValue(), 40_000);
                    }
                }).setParallelism(1);
        env.execute();
    }

    @Test
    public void testBroadcast() throws Exception {
        // Parameters
        int totalValues = 10_000;
        double valueRate = 10.0;
        int valueNodes = 3;
        int vbRatio = 1_000;
        int hbRatio = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(valueNodes + 1);

        DataStream<ValueOrHeartbeat> valueStream =
                env.addSource(new ValueSource(totalValues, valueRate)).setParallelism(valueNodes);
        DataStream<BarrierOrHeartbeat> barrierStream =
                env.addSource(new BarrierSource(totalValues, valueRate, vbRatio, hbRatio)).setParallelism(1);

        final MapStateDescriptor<Void, Void> stateDescriptor =
                new MapStateDescriptor("BroadcastState", Void.class, Void.class);
        BroadcastStream<BarrierOrHeartbeat> broadcastStream = barrierStream.broadcast(stateDescriptor);

        valueStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Tuple2<Long, Long>>() {
                    private long valueTimestamp = Long.MIN_VALUE;
                    private long barrierTimestamp = Long.MIN_VALUE;
                    private long sum = 0;

                    private final Deque<Value> unprocessedValues = new ArrayDeque<>();
                    private final Deque<Barrier> unprocessedBarriers = new ArrayDeque<>();

                    @Override
                    public void processElement(ValueOrHeartbeat item, ReadOnlyContext ctx, Collector<Tuple2<Long, Long>> collector) {
                        item.<Void>match(
                                value -> {
                                    unprocessedValues.addLast(value);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        valueTimestamp = Math.max(valueTimestamp, item.getTimestamp());
                        makeProgress(collector);
                    }

                    @Override
                    public void processBroadcastElement(BarrierOrHeartbeat item, Context ctx, Collector<Tuple2<Long, Long>> collector) {
                        item.<Void>match(
                                barrier -> {
                                    unprocessedBarriers.addLast(barrier);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        barrierTimestamp = Math.max(barrierTimestamp, item.getTimestamp());
                        makeProgress(collector);
                    }

                    private void makeProgress(Collector<Tuple2<Long, Long>> collector) {
                        long currentTime = Math.min(valueTimestamp, barrierTimestamp);
                        while (!unprocessedValues.isEmpty() &&
                                unprocessedValues.getFirst().getTimestamp() <= currentTime) {
                            Value value = unprocessedValues.removeFirst();
                            while (!unprocessedBarriers.isEmpty() &&
                                    unprocessedBarriers.getFirst().getTimestamp() < value.getTimestamp()) {
                                Barrier barrier = unprocessedBarriers.removeFirst();
                                LOG.debug("[{}] collecting {} @ {}", getRuntimeContext().getIndexOfThisSubtask(), sum, barrier.getTimestamp());
                                collector.collect(Tuple2.of(sum, barrier.getTimestamp()));
                                sum = 0;
                            }
                            LOG.debug("[{}] new sum = {} + {} @ {}", getRuntimeContext().getIndexOfThisSubtask(), sum, value.getVal(), value.getTimestamp());
                            sum += value.getVal();
                        }
                        while (!unprocessedBarriers.isEmpty() &&
                                unprocessedBarriers.getFirst().getTimestamp() <= currentTime) {
                            Barrier barrier = unprocessedBarriers.removeFirst();
                            LOG.debug("[{}] collecting {} @ {}", getRuntimeContext().getIndexOfThisSubtask(), sum, barrier.getTimestamp());
                            collector.collect(Tuple2.of(sum, barrier.getTimestamp()));
                            sum = 0;
                        }
                    }

                    @Override
                    public void close() {
                        LOG.info("Closing with {} unprocessed values, {} unprocessed barriers, and sum {}", unprocessedValues.size(), unprocessedBarriers.size(), sum);
                    }
                }).setParallelism(valueNodes)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Long, Long>>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple2<Long, Long> tuple, long l) {
                        return new Watermark(l);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<Long, Long> tuple, long l) {
                        return tuple.f1;
                    }
                })
                .timeWindowAll(Time.milliseconds(vbRatio))
                .reduce((x, y) -> Tuple2.of(x.f0 + y.f0, x.f1))
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void invoke(Tuple2<Long, Long> out, Context ctx) {
                        LOG.info("out: {} @ {}", out.f0, out.f1);
                    }
                }).setParallelism(1);
        env.execute();
    }
}
