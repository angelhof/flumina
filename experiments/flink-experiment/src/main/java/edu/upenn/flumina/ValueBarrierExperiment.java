package edu.upenn.flumina;

import edu.upenn.flumina.data.Barrier;
import edu.upenn.flumina.data.Value;
import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;
import edu.upenn.flumina.sink.TimestampMapper;
import edu.upenn.flumina.source.BarrierSource;
import edu.upenn.flumina.source.ValueSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

public class ValueBarrierExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(ValueBarrierExperiment.class);

    public static void main(String[] args) throws Exception {
        // Parse arguments
        ValueBarrierConfig conf = ValueBarrierConfig.fromArgs(args);

        // Prepare the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(conf.getValueNodes() + 1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // By default, the buffer timeout is 100ms, resulting in median latencies around 100ms.
        // Since Flumina doesn't have the corresponding mechanism for tuning the latency vs. throughput tradeoff,
        // we set this to 0 -- events are immediately flushed from the buffer.
        env.setBufferTimeout(0);

        // Prepare the input streams

        // We sync all the input sources with the same start time, which is current time plus 2000 ms
        long startTime = System.nanoTime() + 2_000_000_000L;

        ValueSource valueSource = new ValueSource(conf.getTotalValues(), conf.getValueRate(), startTime);
        DataStream<ValueOrHeartbeat> valueStream = env.addSource(valueSource)
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("values");
        BarrierSource barrierSource = new BarrierSource(
                conf.getTotalValues(), conf.getValueRate(), conf.getValueBarrierRatio(),
                conf.getHeartbeatRatio(), startTime);
        DataStream<BarrierOrHeartbeat> barrierStream = env.addSource(barrierSource)
                .setParallelism(1)
                .slotSharingGroup("barriers");

        // Broadcast the barrier stream and connect it with the value stream
        // We use a dummy broadcast state descriptor that is never actually used.
        MapStateDescriptor<Void, Void> broadcastStateDescriptor =
                new MapStateDescriptor<>("BroadcastState", Void.class, Void.class);
        BroadcastStream<BarrierOrHeartbeat> broadcastStream = barrierStream.broadcast(broadcastStateDescriptor);

        DataStream<String> output = valueStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Tuple3<Long, Long, Long>>() {
                    private long valuePhysicalTimestamp = Long.MIN_VALUE;
                    private long barrierPhysicalTimestamp = Long.MIN_VALUE;
                    private long sum = 0;

                    private final Deque<Value> unprocessedValues = new ArrayDeque<>();
                    private final Deque<Barrier> unprocessedBarriers = new ArrayDeque<>();

                    @Override
                    public void processElement(ValueOrHeartbeat valueOrHeartbeat,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple3<Long, Long, Long>> collector) {
                        valueOrHeartbeat.<Void>match(
                                value -> {
                                    unprocessedValues.addLast(value);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        valuePhysicalTimestamp = Math.max(valuePhysicalTimestamp, valueOrHeartbeat.getPhysicalTimestamp());
                        makeProgress(collector);
                    }

                    @Override
                    public void processBroadcastElement(BarrierOrHeartbeat barrierOrHeartbeat,
                                                        Context ctx,
                                                        Collector<Tuple3<Long, Long, Long>> collector) {
                        barrierOrHeartbeat.<Void>match(
                                barrier -> {
                                    unprocessedBarriers.addLast(barrier);
                                    return null;
                                },
                                heartbeat -> null
                        );
                        barrierPhysicalTimestamp = Math.max(barrierPhysicalTimestamp, barrierOrHeartbeat.getPhysicalTimestamp());
                        makeProgress(collector);
                    }

                    private void makeProgress(Collector<Tuple3<Long, Long, Long>> collector) {
                        long currentTime = Math.min(valuePhysicalTimestamp, barrierPhysicalTimestamp);
                        while (!unprocessedValues.isEmpty() &&
                                unprocessedValues.getFirst().getPhysicalTimestamp() <= currentTime) {
                            Value value = unprocessedValues.removeFirst();
                            while (!unprocessedBarriers.isEmpty() &&
                                    unprocessedBarriers.getFirst().getPhysicalTimestamp() < value.getPhysicalTimestamp()) {
                                Barrier barrier = unprocessedBarriers.removeFirst();
                                collector.collect(Tuple3.of(sum, barrier.getLogicalTimestamp(), barrier.getPhysicalTimestamp()));
                                sum = 0;
                            }
                            sum += value.getVal();
                        }
                        while (!unprocessedBarriers.isEmpty() &&
                                unprocessedBarriers.getFirst().getPhysicalTimestamp() <= currentTime) {
                            Barrier barrier = unprocessedBarriers.removeFirst();
                            collector.collect(Tuple3.of(sum, barrier.getLogicalTimestamp(), barrier.getPhysicalTimestamp()));
                            sum = 0;
                        }
                    }
                })
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("values")
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<Long, Long, Long>>() {
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple3<Long, Long, Long> tuple, long l) {
                        return new Watermark(l);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<Long, Long, Long> tuple, long l) {
                        // The field tuple.f1 corresponds to the logical timestamp
                        return tuple.f1;
                    }
                })
                .timeWindowAll(Time.milliseconds(conf.getValueBarrierRatio()))
                .reduce((x, y) -> {
                    x.f0 += y.f0;
                    return x;
                })
                .slotSharingGroup("barriers")
                .startNewChain()
                .map(new TimestampMapper())
                .setParallelism(1);
        output.writeAsText(conf.getOutputFile(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        JobExecutionResult result = env.execute("Value-Barrier Experiment");

        try (FileWriter statisticsFile = new FileWriter(conf.getStatisticsFile())) {
            long totalEvents = conf.getValueNodes() * conf.getTotalValues() + conf.getTotalValues() / conf.getValueBarrierRatio();
            long totalTimeMillis = (System.nanoTime() - startTime) / 1_000_000;
            long netRuntimeMillis = result.getNetRuntime(TimeUnit.MILLISECONDS);
            long meanThroughput = Math.floorDiv(totalEvents, netRuntimeMillis);
            long optimalThroughput = (long)(conf.getValueRate() * conf.getValueNodes()
                    + conf.getValueRate() / conf.getValueBarrierRatio());
            statisticsFile.write(String.format("Total time (ms): %d%n", totalTimeMillis));
            statisticsFile.write(String.format("Events processed: %d%n", totalEvents));
            statisticsFile.write(String.format("Net runtime (ms): %d%n", netRuntimeMillis));
            statisticsFile.write(String.format("Mean throughput (events/ms): %d%n", meanThroughput));
            statisticsFile.write(String.format("Optimal throughput (events/ms): %d%n", optimalThroughput));
        } catch (IOException e) {
            LOG.error("Exception while trying to write to {}: {}", conf.getStatisticsFile(), e.getMessage());
        }
    }
}
