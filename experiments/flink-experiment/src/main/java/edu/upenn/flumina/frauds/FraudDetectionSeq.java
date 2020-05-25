package edu.upenn.flumina.frauds;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.FraudDetectionConfig;
import edu.upenn.flumina.valuebarrier.BarrierSource;
import edu.upenn.flumina.valuebarrier.ValueSource;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;
import edu.upenn.flumina.valuebarrier.data.Value;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static edu.upenn.flumina.time.TimeHelper.min;

public class FraudDetectionSeq implements Experiment {

    private final FraudDetectionConfig conf;

    public FraudDetectionSeq(final FraudDetectionConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(conf.getValueNodes() + 1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final var valueSource = new ValueSource(conf.getTotalValues(), conf.getValueRate(), startTime);
        final var valueStream = env.addSource(valueSource)
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("values");
        final var barrierSource = new BarrierSource(
                conf.getTotalValues(), conf.getValueRate(), conf.getValueBarrierRatio(),
                conf.getHeartbeatRatio(), startTime);
        final var barrierStream = env.addSource(barrierSource)
                .setParallelism(1)
                .slotSharingGroup("barriers");

        final var unprocessedValuesDescriptor = new ValueStateDescriptor<>("UnprocessedValues",
                TypeInformation.of(new TypeHint<Deque<Value>>() {
                }));
        final var unprocessedBarriersDescriptor = new ValueStateDescriptor<>("UnprocessedBarriers",
                TypeInformation.of(new TypeHint<Deque<Barrier>>() {
                }));
        final var valueTimestampDescriptor = new ValueStateDescriptor<>("ValueTimestamp",
                TypeInformation.of(Instant.class));
        final var barrierTimestampDescriptor = new ValueStateDescriptor<>("BarrierTimestamp",
                TypeInformation.of(Instant.class));
        final var sumDescriptor = new ValueStateDescriptor<>("Sum",
                TypeInformation.of(Long.class));

        barrierStream.keyBy(x -> 0)
                .connect(valueStream.keyBy(x -> 0))
                .process(new KeyedCoProcessFunction<Integer, BarrierOrHeartbeat, ValueOrHeartbeat, Tuple3<String, Long, Instant>>() {

                    private ValueState<Instant> valueTimestampState;
                    private ValueState<Instant> barrierTimestampState;
                    private ValueState<Long> sumState;
                    private ValueState<Deque<Value>> unprocessedValuesState;
                    private ValueState<Deque<Barrier>> unprocessedBarriersState;

                    @Override
                    public void open(final Configuration parameters) {
                        valueTimestampState = getRuntimeContext().getState(valueTimestampDescriptor);
                        barrierTimestampState = getRuntimeContext().getState(barrierTimestampDescriptor);
                        sumState = getRuntimeContext().getState(sumDescriptor);
                        unprocessedValuesState = getRuntimeContext().getState(unprocessedValuesDescriptor);
                        unprocessedBarriersState = getRuntimeContext().getState(unprocessedBarriersDescriptor);
                    }

                    @Override
                    public void processElement1(final BarrierOrHeartbeat barrierOrHeartbeat,
                                                final Context ctx,
                                                final Collector<Tuple3<String, Long, Instant>> out) throws Exception {
                        updateUnprocessedElements(unprocessedBarriersState,
                                barrierOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
                        updateTimestamp(barrierTimestampState, barrierOrHeartbeat.getPhysicalTimestamp());
                        makeProgress(out);
                    }

                    @Override
                    public void processElement2(final ValueOrHeartbeat valueOrHeartbeat,
                                                final Context ctx,
                                                final Collector<Tuple3<String, Long, Instant>> out) throws Exception {
                        updateUnprocessedElements(unprocessedValuesState,
                                valueOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
                        updateTimestamp(valueTimestampState, valueOrHeartbeat.getPhysicalTimestamp());
                        makeProgress(out);
                    }

                    private <T> void updateUnprocessedElements(final ValueState<Deque<T>> unprocessedElementsState,
                                                               final List<T> listToAdd) throws IOException {
                        if (unprocessedElementsState.value() == null) {
                            unprocessedElementsState.update(new ArrayDeque<>());
                        }
                        unprocessedElementsState.value().addAll(listToAdd);
                    }

                    private void updateTimestamp(final ValueState<Instant> timestampState,
                                                 final Instant timestamp) throws IOException {
                        if (timestampState.value() == null) {
                            timestampState.update(Instant.MIN);
                        }
                        if (timestampState.value().isBefore(timestamp)) {
                            timestampState.update(timestamp);
                        }
                    }

                    private void makeProgress(final Collector<Tuple3<String, Long, Instant>> out) throws IOException {
                        final Instant currentTime = min(valueTimestampState.value(), barrierTimestampState.value());
                        final Deque<Value> unprocessedValues = unprocessedValuesState.value();
                        final Deque<Barrier> unprocessedBarriers = unprocessedBarriersState.value();
                        while (!unprocessedValues.isEmpty() &&
                                unprocessedValues.getFirst().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
                            final Value value = unprocessedValues.removeFirst();
                            while (!unprocessedBarriers.isEmpty() &&
                                    unprocessedBarriers.getFirst().getPhysicalTimestamp().isBefore(value.getPhysicalTimestamp())) {
                                final Barrier barrier = unprocessedBarriers.removeFirst();
                                if (sumState.value() == null) {
                                    sumState.update(0L);
                                }
                                out.collect(Tuple3.of("Barrier", sumState.value(), barrier.getPhysicalTimestamp()));
                                sumState.update(0L);
                            }
                            // TODO: Change to make this the full value-barrier example
                            if (sumState.value() == null) {
                                sumState.update(0L);
                            }
                            sumState.update(sumState.value() + value.getVal());
                        }
                        while (!unprocessedBarriers.isEmpty() &&
                                unprocessedBarriers.getFirst().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
                            final Barrier barrier = unprocessedBarriers.removeFirst();
                            if (sumState.value() == null) {
                                sumState.update(0L);
                            }
                            out.collect(Tuple3.of("Barrier", sumState.value(), barrier.getPhysicalTimestamp()));
                            sumState.update(0L);
                        }
                    }
                })
                .setParallelism(1)
                .slotSharingGroup("barriers")
                .map(new TimestampMapper())
                .writeAsText(conf.getOutFile(), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

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
