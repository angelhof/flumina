package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.Union;
import edu.upenn.flumina.pageview.data.Heartbeat;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeats;

import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PageViewGenerator implements GeneratorWithHeartbeats<PageView, Heartbeat> {

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
        final var pageViewStream = LongStream.range(0, totalEvents)
                .mapToObj(this::generatePageViewStream)
                .flatMap(Function.identity());
        final var withFinalHeartbeat =
                Stream.concat(pageViewStream, Stream.of(new PageViewHeartbeat(totalEvents, Long.MAX_VALUE)));
        return withFinalHeartbeat.iterator();
    }

    private Stream<Union<PageView, Heartbeat>> generatePageViewStream(final long logicalTimestamp) {
        return UserIdHelper.getUserIds(totalUsers).stream()
                .map(userId -> new PageView(userId, logicalTimestamp));
    }

}
