package edu.upenn.flumina.frauds;

import edu.upenn.flumina.remote.ForkJoinService;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FraudDetectionService implements ForkJoinService<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private final int transactionParallelism;
    private final List<Semaphore> joinSemaphores;
    private final List<Semaphore> forkSemaphores;
    private final List<Tuple2<Long, Long>> states;

    public FraudDetectionService(final int transactionParallelism) {
        this.transactionParallelism = transactionParallelism;
        joinSemaphores = IntStream.range(0, transactionParallelism)
                .mapToObj(x -> new Semaphore(0)).collect(Collectors.toList());
        forkSemaphores = IntStream.range(0, transactionParallelism)
                .mapToObj(x -> new Semaphore(0)).collect(Collectors.toList());
        states = IntStream.range(0, transactionParallelism)
                .mapToObj(x -> Tuple2.of(0L, 0L)).collect(Collectors.toList());
    }

    @Override
    public Tuple2<Long, Long> joinChild(final int subtaskIndex, final Tuple2<Long, Long> state) {
        states.set(subtaskIndex, state);
        joinSemaphores.get(subtaskIndex).release();
        forkSemaphores.get(subtaskIndex).acquireUninterruptibly();
        return states.get(subtaskIndex);
    }

    @Override
    public Tuple2<Long, Long> joinParent(final int subtaskIndex, final Tuple2<Long, Long> state) {
        long currentSum = 0L;
        for (int i = 0; i < transactionParallelism; ++i) {
            joinSemaphores.get(i).acquireUninterruptibly();
            currentSum += states.get(i).f1;
        }

        // We use the fact that all the children have the same previous sum, which otherwise we would
        // get from the state passed as an argument. As a consequence, we're not using state at all, and
        // the caller is free to make it null. Alternatively, we could have the children pass only their
        // current sums, and get the previous sum from state passed here as an argument.
        final long previousSum = states.get(0).f0;
        for (int i = transactionParallelism - 1; i >= 0; --i) {
            final var previousAndCurrentSum = states.get(i);
            previousAndCurrentSum.f0 = currentSum;
            previousAndCurrentSum.f1 = 0L;
            forkSemaphores.get(i).release();
        }
        return Tuple2.of(previousSum, currentSum);
    }

}
