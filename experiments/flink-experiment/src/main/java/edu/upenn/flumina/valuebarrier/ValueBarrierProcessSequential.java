package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.util.TimestampComparator;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;
import edu.upenn.flumina.valuebarrier.data.Value;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.*;

import static edu.upenn.flumina.time.TimeHelper.min;

public class ValueBarrierProcessSequential extends
        CoProcessFunction<ValueOrHeartbeat, BarrierOrHeartbeat, Tuple3<Long, Long, Instant>> {

    private final List<Instant> valueTimestamps = new ArrayList<>();
    private final PriorityQueue<Value> values = new PriorityQueue<>(new TimestampComparator());
    private final Queue<Barrier> barriers = new ArrayDeque<>();
    private Instant barrierTimestamp = Instant.MIN;
    private long sum = 0L;

    public ValueBarrierProcessSequential(final int valueParallelism) {
        valueTimestamps.addAll(Collections.nCopies(valueParallelism, Instant.MIN));
    }

    @Override
    public void processElement1(final ValueOrHeartbeat valueOrHeartbeat,
                                final Context ctx,
                                final Collector<Tuple3<Long, Long, Instant>> out) {
        values.addAll(valueOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        valueTimestamps.set(valueOrHeartbeat.getSourceIndex(), valueOrHeartbeat.getPhysicalTimestamp());
        makeProgress(out);
    }

    @Override
    public void processElement2(final BarrierOrHeartbeat barrierOrHeartbeat,
                                final Context ctx,
                                final Collector<Tuple3<Long, Long, Instant>> out) {
        barriers.addAll(barrierOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        barrierTimestamp = barrierOrHeartbeat.getPhysicalTimestamp();
        makeProgress(out);
    }

    private Instant getCurrentTimestamp() {
        final var valueTimestamp = valueTimestamps.stream().min(Instant::compareTo).get();
        return min(valueTimestamp, barrierTimestamp);
    }

    private void makeProgress(final Collector<Tuple3<Long, Long, Instant>> out) {
        final var currentTimestamp = getCurrentTimestamp();
        while (!barriers.isEmpty() &&
                barriers.element().getPhysicalTimestamp().compareTo(currentTimestamp) <= 0) {
            final var barrier = barriers.remove();
            while (!values.isEmpty() &&
                    values.element().getPhysicalTimestamp().isBefore(barrier.getPhysicalTimestamp())) {
                update(values.remove());
            }
            update(barrier, out);
        }
        while (!values.isEmpty() &&
                values.element().getPhysicalTimestamp().compareTo(currentTimestamp) <= 0) {
            update(values.remove());
        }
    }

    private void update(final Value value) {
        sum += value.val;
    }

    private void update(final Barrier barrier,
                        final Collector<Tuple3<Long, Long, Instant>> out) {
        out.collect(Tuple3.of(sum, barrier.getLogicalTimestamp(), barrier.getPhysicalTimestamp()));
        sum = 0L;
    }

}
