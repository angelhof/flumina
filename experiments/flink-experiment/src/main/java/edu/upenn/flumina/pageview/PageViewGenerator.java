package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.Union;
import edu.upenn.flumina.pageview.data.Heartbeat;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeats;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PageViewGenerator implements GeneratorWithHeartbeats<PageView, Heartbeat> {

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
    public Iterator<Union<PageView, Heartbeat>> getIterator() {
        final Stream<Union<PageView, Heartbeat>> pageViewStream = LongStream.range(0, totalEvents)
                .mapToObj(ts -> {
                    final int nextUserId = random.nextInt(totalUsers);
                    return new PageView(nextUserId, ts);
                });
        final Stream<Union<PageView, Heartbeat>> withFinalHeartbeat =
                Stream.concat(pageViewStream, Stream.of(new PageViewHeartbeat(totalEvents, Long.MAX_VALUE)));
        return withFinalHeartbeat.iterator();
    }

}
