package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewOrHeartbeat;
import edu.upenn.flumina.pageview.data.Update;
import edu.upenn.flumina.util.TimestampComparator;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.*;

import static edu.upenn.flumina.time.TimeHelper.min;

public class PageViewProcessSequential extends CoProcessFunction<GetOrUpdateOrHeartbeat, PageViewOrHeartbeat, Update> {

    private final List<Instant> pageViewTimestamps = new ArrayList<>();
    private final List<Integer> zipCodes = new ArrayList<>();
    private final Queue<Update> updates = new ArrayDeque<>();
    private final PriorityQueue<PageView> pageViews = new PriorityQueue<>(new TimestampComparator());
    private Instant updateTimestamp = Instant.MIN;

    public PageViewProcessSequential(final int totalUsers, final int pageViewParallelism) {
        pageViewTimestamps.addAll(Collections.nCopies(pageViewParallelism, Instant.MIN));
        zipCodes.addAll(Collections.nCopies(totalUsers, 10_000));
    }

    @Override
    public void processElement1(final GetOrUpdateOrHeartbeat getOrUpdateOrHeartbeat,
                                final Context ctx,
                                final Collector<Update> out) {
        updates.addAll(getOrUpdateOrHeartbeat.match(
                gou -> gou.match(g -> Collections.emptyList(), List::of),
                hb -> Collections.emptyList()));
        updateTimestamp = getOrUpdateOrHeartbeat.getPhysicalTimestamp();
        makeProgress(out);
    }

    @Override
    public void processElement2(final PageViewOrHeartbeat pageViewOrHeartbeat,
                                final Context ctx,
                                final Collector<Update> out) {
        pageViews.addAll(pageViewOrHeartbeat.match(List::of, hb -> Collections.emptyList()));
        pageViewTimestamps.set(pageViewOrHeartbeat.getSourceIndex(), pageViewOrHeartbeat.getPhysicalTimestamp());
        makeProgress(out);
    }

    private Instant getCurrentTimestamp() {
        final var pageViewTimestamp = pageViewTimestamps.stream().min(Instant::compareTo).get();
        return min(pageViewTimestamp, updateTimestamp);
    }

    private void makeProgress(final Collector<Update> out) {
        final var currentTimestamp = getCurrentTimestamp();
        while (!updates.isEmpty() &&
                updates.element().getPhysicalTimestamp().compareTo(currentTimestamp) <= 0) {
            final var update = updates.remove();
            while (!pageViews.isEmpty() &&
                    pageViews.element().getPhysicalTimestamp().isBefore(update.getPhysicalTimestamp())) {
                update(pageViews.remove(), out);
            }
            update(update, out);
        }
        while (!pageViews.isEmpty() &&
                pageViews.element().getPhysicalTimestamp().compareTo(currentTimestamp) <= 0) {
            update(pageViews.remove(), out);
        }
    }

    private void update(final Update update, final Collector<Update> out) {
        zipCodes.set(update.getUserId(), update.zipCode);
        out.collect(update);
    }

    private void update(final PageView pageView, final Collector<Update> out) {
        // This update is a no-op
    }

}