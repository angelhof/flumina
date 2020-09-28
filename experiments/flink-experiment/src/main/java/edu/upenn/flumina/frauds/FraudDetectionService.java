package edu.upenn.flumina.frauds;

import edu.upenn.flumina.remote.ForkJoinService;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FraudDetectionService implements ForkJoinService<Tuple2<Long, Long>> {

    private final AtomicInteger idAssigner = new AtomicInteger();

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
    public int getChildId() {
        return idAssigner.getAndIncrement();
    }

    @Override
    public Tuple2<Long, Long> joinChild(final int childId, final Tuple2<Long, Long> state) {
        states.set(childId, state);
        joinSemaphores.get(childId).release();
        forkSemaphores.get(childId).acquireUninterruptibly();
        return states.get(childId);
    }

    @Override
    public Tuple2<Long, Long> joinParent() {
        long currentSum = 0L;
        for (int i = 0; i < transactionParallelism; ++i) {
            joinSemaphores.get(i).acquireUninterruptibly();
            currentSum += states.get(i).f1;
        }
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
