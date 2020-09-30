package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.Experiment;
import edu.upenn.flumina.config.ValueBarrierConfig;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.Value;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

import static edu.upenn.flumina.time.TimeHelper.toEpochMilli;

public class ValueBarrierSequentialExperiment implements Experiment {

    private static final Logger LOG = LoggerFactory.getLogger(ValueBarrierSequentialExperiment.class);

    private final ValueBarrierConfig conf;

    public ValueBarrierSequentialExperiment(final ValueBarrierConfig conf) {
        this.conf = conf;
    }

    @Override
    public JobExecutionResult run(final StreamExecutionEnvironment env, final Instant startTime) throws Exception {
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final var valueSource = new ValueSource(conf.getTotalValues(), conf.getValueRate(), startTime);
        final var valueStream = env.addSource(valueSource)
                .setParallelism(conf.getValueNodes())
                .slotSharingGroup("values");
        final var barrierSource = new BarrierSource(
                conf.getTotalValues(), conf.getValueRate(), conf.getValueBarrierRatio(),
                conf.getHeartbeatRatio(), startTime);
        final var barrierStream = env.addSource(barrierSource)
                .slotSharingGroup("barriers");

        final var valuesDescriptor = new ValueStateDescriptor<>("UnprocessedValues",
                TypeInformation.of(new TypeHint<PriorityQueue<Value>>() {
                }));
        final var barriersDescriptor = new ValueStateDescriptor<>("UnprocessedBarriers",
                TypeInformation.of(new TypeHint<Queue<Barrier>>() {
                }));
        final var sumDescriptor = new ValueStateDescriptor<>("Sum", TypeInformation.of(Long.class));

        valueStream.keyBy(x -> 0)
                .connect(barrierStream.keyBy(x -> 0))
                .process(new KeyedCoProcessFunction<Integer, Value, Barrier, Tuple3<Long, Long, Instant>>() {

                    private transient ValueState<Long> sumState;
                    private transient ValueState<PriorityQueue<Value>> unprocessedValuesState;
                    private transient ValueState<Queue<Barrier>> unprocessedBarriersState;

                    @Override
                    public void open(final Configuration parameters) {
                        sumState = getRuntimeContext().getState(sumDescriptor);
                        unprocessedValuesState = getRuntimeContext().getState(valuesDescriptor);
                        unprocessedBarriersState = getRuntimeContext().getState(barriersDescriptor);
                    }

                    @Override
                    public void processElement1(final Value value,
                                                final Context ctx,
                                                final Collector<Tuple3<Long, Long, Instant>> collector) throws IOException {
                        getUnprocessedValues().add(value);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                    }

                    @Override
                    public void processElement2(final Barrier barrier,
                                                final Context ctx,
                                                final Collector<Tuple3<Long, Long, Instant>> collector) throws IOException {
                        getUnprocessedBarriers().add(barrier);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
                    }

                    private PriorityQueue<Value> getUnprocessedValues() throws IOException {
                        if (unprocessedValuesState.value() == null) {
                            unprocessedValuesState.update(new PriorityQueue<>(
                                    Comparator.comparing(Value::getPhysicalTimestamp)));
                        }
                        return unprocessedValuesState.value();
                    }

                    private Queue<Barrier> getUnprocessedBarriers() throws IOException {
                        if (unprocessedBarriersState.value() == null) {
                            unprocessedBarriersState.update(new ArrayDeque<>());
                        }
                        return unprocessedBarriersState.value();
                    }

                    private long getSum() throws IOException {
                        if (sumState.value() == null) {
                            sumState.update(0L);
                        }
                        return sumState.value();
                    }

                    @Override
                    public void onTimer(final long timestamp,
                                        final OnTimerContext ctx,
                                        final Collector<Tuple3<Long, Long, Instant>> out) throws IOException {
                        final var unprocessedValues = getUnprocessedValues();
                        final var unprocessedBarriers = getUnprocessedBarriers();
                        while (!unprocessedValues.isEmpty() &&
                                toEpochMilli(unprocessedValues.element().getPhysicalTimestamp()) <= timestamp) {
                            final var value = unprocessedValues.remove();
                            while (!unprocessedBarriers.isEmpty() &&
                                    unprocessedBarriers.element().getPhysicalTimestamp()
                                            .isBefore(value.getPhysicalTimestamp())) {
                                update(unprocessedBarriers.remove(), out);
                            }
                            update(value);
                        }
                        while (!unprocessedBarriers.isEmpty() &&
                                toEpochMilli(unprocessedBarriers.element().getPhysicalTimestamp()) <= timestamp) {
                            update(unprocessedBarriers.remove(), out);
                        }
                    }

                    private void update(final Value value) throws IOException {
                        sumState.update(getSum() + value.val);
                    }

                    private void update(final Barrier barrier,
                                        final Collector<Tuple3<Long, Long, Instant>> out) throws IOException {
                        out.collect(Tuple3.of(getSum(), barrier.getLogicalTimestamp(), barrier.getPhysicalTimestamp()));
                        sumState.update(0L);
                    }
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
