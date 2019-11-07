package edu.upenn.flumina;

import edu.upenn.flumina.data.Barrier;
import edu.upenn.flumina.data.Value;
import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;
import edu.upenn.flumina.source.BarrierSource;
import edu.upenn.flumina.source.ValueSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class ValueBarrierExperiment {

    private static final Logger LOG = LogManager.getLogger();

    public static void main(String[] args) {
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
        MapStateDescriptor<Void, Long> barrierTimestampDescriptor =
                new MapStateDescriptor("BarrierTimestamp", Void.class, Long.class);
        BroadcastStream<BarrierOrHeartbeat> broadcastStream = barrierStream.broadcast(barrierTimestampDescriptor);
        valueStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Long>() {
                    private long valueTimestamp = Long.MIN_VALUE;
                    private long barrierTimestamp = Long.MIN_VALUE;
                    private long sum = 0;

                    private final Deque<Value> unprocessedValues = new ArrayDeque<>();
                    private final Deque<Barrier> unprocessedBarriers = new ArrayDeque<>();

                    @Override
                    public void processElement(ValueOrHeartbeat valueOrHeartbeat,
                                               ReadOnlyContext readOnlyContext,
                                               Collector<Long> collector) {
                        valueOrHeartbeat.<Void>match(
                                value -> {
                                    unprocessedValues.addLast(value);
                                    valueTimestamp = value.getTimestamp();
                                    return null;
                                },
                                heartbeat -> {
                                    valueTimestamp = heartbeat.getTimestamp();
                                    return null;
                                }
                        );
                        makeProgress(collector);
                    }

                    @Override
                    public void processBroadcastElement(BarrierOrHeartbeat barrierOrHeartbeat,
                                                        Context context,
                                                        Collector<Long> collector) {
                        barrierOrHeartbeat.<Void>match(
                                barrier -> {
                                    unprocessedBarriers.addLast(barrier);
                                    barrierTimestamp = barrier.getTimestamp();
                                    return null;
                                },
                                heartbeat -> {
                                    barrierTimestamp = heartbeat.getTimestamp();
                                    return null;
                                }
                        );
                        makeProgress(collector);
                    }

                    private void makeProgress(Collector<Long> collector) {
                        long currentTimestamp = Math.min(valueTimestamp, barrierTimestamp);
                        while (!unprocessedValues.isEmpty() &&
                                unprocessedValues.getFirst().getTimestamp() <= currentTimestamp) {
                            Value value = unprocessedValues.removeFirst();
                            while (!unprocessedBarriers.isEmpty() &&
                                    unprocessedBarriers.getFirst().getTimestamp() < value.getTimestamp()) {
                                unprocessedBarriers.removeFirst();
                                collector.collect(sum);
                                sum = 0;
                            }
                            sum += value.getVal();
                        }
                    }
                });
    }
}
