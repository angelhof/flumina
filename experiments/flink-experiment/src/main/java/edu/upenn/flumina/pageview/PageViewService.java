package edu.upenn.flumina.pageview;

import edu.upenn.flumina.remote.ForkJoinService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

public class PageViewService implements ForkJoinService<Integer, Integer> {

    private final int pageViewParallelism;
    private final List<List<Semaphore>> forkSemaphores;
    private final List<List<Semaphore>> joinSemaphores;
    private final List<Integer> zipCode;

    public PageViewService(final int totalUsers, final int pageViewParallelism) {
        this.pageViewParallelism = pageViewParallelism;
        forkSemaphores = new ArrayList<>(totalUsers);
        joinSemaphores = new ArrayList<>(totalUsers);
        zipCode = new ArrayList<>(totalUsers);
        for (int i = 0; i < totalUsers; ++i) {
            final var forkSems = new ArrayList<Semaphore>(pageViewParallelism);
            final var joinSems = new ArrayList<Semaphore>(pageViewParallelism);
            for (int j = 0; j < pageViewParallelism; ++j) {
                forkSems.add(new Semaphore(0));
                joinSems.add(new Semaphore(0));
            }
            forkSemaphores.add(forkSems);
            joinSemaphores.add(joinSems);
            zipCode.add(10_000);
        }
    }

    @Override
    public Integer joinChild(final int subtaskIndex, final Integer state) {
        final int parentId = subtaskIndex / pageViewParallelism;
        final int childId = subtaskIndex % pageViewParallelism;
        joinSemaphores.get(parentId).get(childId).release();
        forkSemaphores.get(parentId).get(childId).acquireUninterruptibly();
        return zipCode.get(parentId);
    }

    @Override
    public Integer joinParent(final int subtaskIndex, final Integer state) {
        joinSemaphores.get(subtaskIndex).forEach(Semaphore::acquireUninterruptibly);
        zipCode.set(subtaskIndex, state);
        // We iterate over forkSemaphores backwards, to simulate children organized in a chain
        for (int i = forkSemaphores.get(subtaskIndex).size() - 1; i >= 0; --i) {
            forkSemaphores.get(subtaskIndex).get(i).release();
        }
        return state;
    }

}
