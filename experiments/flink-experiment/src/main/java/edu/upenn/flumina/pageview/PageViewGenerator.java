package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Generator;
import edu.upenn.flumina.pageview.data.Heartbeat;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewOrHeartbeat;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PageViewGenerator implements Generator<PageViewOrHeartbeat> {

    // TODO: Perhaps have a centralized random?
    private final Random random = new Random();

    private final int totalEvents;
    private final int totalUsers;
    private final double rate;

    public PageViewGenerator(final int totalEvents, final int totalUsers, final double rate) {
        this.totalEvents = totalEvents;
        this.totalUsers = totalUsers;
        this.rate = rate;
    }

    @Override
    public double getRate() {
        return rate;
    }

    @Override
    public Iterator<PageViewOrHeartbeat> getIterator() {
        final Stream<PageViewOrHeartbeat> pageViewStream = LongStream.range(0, totalEvents)
                .mapToObj(ts -> {
                    final int nextUserId = random.nextInt(totalUsers);
                    return new PageView(nextUserId, ts);
                });
        final Stream<PageViewOrHeartbeat> withFinalHeartbeat =
                Stream.concat(pageViewStream, Stream.of(new Heartbeat(totalEvents, Long.MAX_VALUE)));
        return withFinalHeartbeat.iterator();
    }

}
