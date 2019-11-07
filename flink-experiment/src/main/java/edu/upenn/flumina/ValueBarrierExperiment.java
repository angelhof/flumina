package edu.upenn.flumina;

import edu.upenn.flumina.data.Barrier;
import edu.upenn.flumina.data.Value;
import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;
import edu.upenn.flumina.source.BarrierSource;
import edu.upenn.flumina.source.ValueSource;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;

public class ValueBarrierExperiment {

    private static final Logger LOG = LogManager.getLogger();

    public static void main(String[] args) throws Exception {
        // Parse arguments
        ValueBarrierConfig conf = ValueBarrierConfig.fromArgs(args);

        // Prepare the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(conf.getValueNodes() + 1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Prepare the input streams
        ValueSource valueSource = new ValueSource(conf.getTotalValues(), conf.getValueRate());
        DataStream<ValueOrHeartbeat> valueStream = env.addSource(valueSource).setParallelism(conf.getValueNodes());
        BarrierSource barrierSource = new BarrierSource(
                conf.getTotalValues(), conf.getValueRate(), conf.getValueBarrierRatio(), conf.getHeartbeatRatio());
        DataStream<BarrierOrHeartbeat> barrierStream = env.addSource(barrierSource).setParallelism(1);

        // Broadcast the barrier stream and connect it with the value stream
        // We use a dummy broadcast state descriptor that is never actually used.
        MapStateDescriptor<Void, Void> broadcastStateDescriptor =
                new MapStateDescriptor("BroadcastState", Void.class, Void.class);
        BroadcastStream<BarrierOrHeartbeat> broadcastStream = barrierStream.broadcast(broadcastStateDescriptor);

        DataStream<Tuple2<Long, Long>> output = valueStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Tuple2<Long, Long>>() {
                    private long valueTimestamp = Long.MIN_VALUE;
                    private long barrierTimestamp = Long.MIN_VALUE;
                    private long sum = 0;

                    private final Deque<Value> unprocessedValues = new ArrayDeque<>();
                    private final Deque<Barrier> unprocessedBarriers = new ArrayDeque<>();

                    @Override
                    public void processElement(ValueOrHeartbeat valueOrHeartbeat,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<Long, Long>> collector) {
                        valueOrHeartbeat.<Void>match(
                                value -> {
                                    unprocessedValues.addLast(value);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        valueTimestamp = Math.max(valueTimestamp, valueOrHeartbeat.getTimestamp());
                        makeProgress(collector);
                    }

                    @Override
                    public void processBroadcastElement(BarrierOrHeartbeat barrierOrHeartbeat,
                                                        Context ctx,
                                                        Collector<Tuple2<Long, Long>> collector) {
                        barrierOrHeartbeat.<Void>match(
                                barrier -> {
                                    unprocessedBarriers.addLast(barrier);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        barrierTimestamp = Math.max(barrierTimestamp, barrierOrHeartbeat.getTimestamp());
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
                                collector.collect(Tuple2.of(sum, barrier.getTimestamp()));
                                sum = 0;
                            }
                            sum += value.getVal();
                        }
                        while (!unprocessedBarriers.isEmpty() &&
                                unprocessedBarriers.getFirst().getTimestamp() <= currentTime) {
                            Barrier barrier = unprocessedBarriers.removeFirst();
                            collector.collect(Tuple2.of(sum, barrier.getTimestamp()));
                            sum = 0;
                        }
                    }
                }).setParallelism(conf.getValueNodes())
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
                .timeWindowAll(Time.milliseconds(conf.getValueBarrierRatio()))
                .reduce((x, y) -> {
                    x.f0 += y.f0;
                    return x;
                });

        StreamingFileSink<Tuple2<Long, Long>> sink = StreamingFileSink
                .forRowFormat(
                        new Path(conf.getOutputPath()),
                        (Encoder<Tuple2<Long, Long>>) (tuple, out) -> {
                            final StringBuilder sb = new StringBuilder();
                            // TODO: Add timestamp (look at java.time)
                            sb.append(tuple.f0).append(" @ ").append(tuple.f1).append('\n');
                            out.write(sb.toString().getBytes());
                        }
                )
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();
        output.addSink(sink).setParallelism(1);

        env.execute();
    }
}
