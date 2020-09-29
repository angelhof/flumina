package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewOrHeartbeat;
import edu.upenn.flumina.pageview.data.Update;
import edu.upenn.flumina.remote.ForkJoinService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static edu.upenn.flumina.time.TimeHelper.max;
import static edu.upenn.flumina.time.TimeHelper.min;

public class PageViewProcessManual extends
        KeyedBroadcastProcessFunction<Integer, PageViewOrHeartbeat, GetOrUpdateOrHeartbeat, Void> {

    private Instant pageViewPhysicalTimestamp = Instant.MIN;
    private Instant getOrUpdatePhysicalTimestamp = Instant.MIN;
    private int zipCode = 10_000;

    private final Queue<PageView> pageViews = new ArrayDeque<>();
    private final Queue<Update> updates = new ArrayDeque<>();

    private final String rmiHost;
    private final String pageViewServiceName;
    private final int pageViewParallelism;
    private int userId;
    private transient ForkJoinService<Integer, Integer> pageViewService;

    public PageViewProcessManual(final String rmiHost, final String pageViewServiceName, final int pageViewParallelism) {
        this.rmiHost = rmiHost;
        this.pageViewServiceName = pageViewServiceName;
        this.pageViewParallelism = pageViewParallelism;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(final Configuration parameters) throws RemoteException, NotBoundException {
        final var registry = LocateRegistry.getRegistry(rmiHost);
        pageViewService = (ForkJoinService<Integer, Integer>) registry.lookup(pageViewServiceName);
        userId = getRuntimeContext().getIndexOfThisSubtask() / pageViewParallelism;
    }

    @Override
    public void processElement(final PageViewOrHeartbeat pageViewOrHeartbeat,
                               final ReadOnlyContext ctx,
                               final Collector<Void> out) throws RemoteException {
        pageViews.addAll(pageViewOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        pageViewPhysicalTimestamp = max(pageViewPhysicalTimestamp, pageViewOrHeartbeat.getPhysicalTimestamp());
        makeProgress();
    }

    @Override
    public void processBroadcastElement(final GetOrUpdateOrHeartbeat getOrUpdateOrHeartbeat,
                                        final Context ctx,
                                        final Collector<Void> out) throws RemoteException {
        // We are getting all GetOrUpdate events, even if they have the wrong userId.
        // We filter out the wrong events.
        if (getOrUpdateOrHeartbeat.match(gou -> gou.getUserId() != userId, hb -> false)) {
            return;
        }
        updates.addAll(getOrUpdateOrHeartbeat.match(
                gou -> gou.match(g -> Collections.emptyList(), List::of),
                hb -> Collections.emptyList()));
        getOrUpdatePhysicalTimestamp = max(getOrUpdatePhysicalTimestamp, getOrUpdateOrHeartbeat.getPhysicalTimestamp());
        makeProgress();
    }

    private void makeProgress() throws RemoteException {
        final var currentTime = min(pageViewPhysicalTimestamp, getOrUpdatePhysicalTimestamp);
        while (!updates.isEmpty() &&
                updates.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
            final var update = updates.remove();
            while (!pageViews.isEmpty() &&
                    pageViews.element().getPhysicalTimestamp().isBefore(update.getPhysicalTimestamp())) {
                process(pageViews.remove());
            }
            join(update);
        }
        while (!pageViews.isEmpty() &&
                pageViews.element().getPhysicalTimestamp().compareTo(currentTime) <= 0) {
            process(pageViews.remove());
        }
    }

    private void process(final PageView pageView) {
        // This update is a no-op
    }

    private void join(final Update update) throws RemoteException {
        zipCode = pageViewService.joinChild(getIterationRuntimeContext().getIndexOfThisSubtask(), zipCode);
    }

}
