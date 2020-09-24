package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.remote.ForkJoinService;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ValueBarrierService implements ForkJoinService<Long> {

    private final AtomicInteger idAssigner = new AtomicInteger();

    private final int valueParallelism;
    private final List<Semaphore> joinSemaphores;
    private final List<Semaphore> forkSemaphores;
    private final long[] states;

    public ValueBarrierService(final int valueParallelism) {
        this.valueParallelism = valueParallelism;
        joinSemaphores = IntStream.range(0, valueParallelism)
                .mapToObj(x -> new Semaphore(0)).collect(Collectors.toList());
        forkSemaphores = IntStream.range(0, valueParallelism)
                .mapToObj(x -> new Semaphore(0)).collect(Collectors.toList());
        states = new long[valueParallelism];
    }

    @Override
    public int getChildId() {
        return idAssigner.getAndIncrement();
    }

    @Override
    public Long joinChild(final int childId, final Long state) {
        states[childId] = state;
        joinSemaphores.get(childId).release();
        forkSemaphores.get(childId).acquireUninterruptibly();
        return states[childId];
    }

    @Override
    public Long joinParent() {
        long sum = 0L;
        for (int i = 0; i < valueParallelism; ++i) {
            joinSemaphores.get(i).acquireUninterruptibly();
            sum += states[i];
        }
        // Simulate fork propagation in the opposite direction
        for (int i = valueParallelism - 1; i >= 0; --i) {
            states[i] = 0;
            forkSemaphores.get(i).release();
        }
        return sum;
    }

}
