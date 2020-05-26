package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.TimestampedUnion;
import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewHeartbeat;
import edu.upenn.flumina.source.GeneratorWithHeartbeats;

import java.time.Instant;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PageViewGenerator implements GeneratorWithHeartbeats<PageView, PageViewHeartbeat> {

    private static final long serialVersionUID = 2311453754387707501L;

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
    public Iterator<TimestampedUnion<PageView, PageViewHeartbeat>> getIterator() {
        final var pageViewStream = LongStream.range(0, totalEvents)
                .mapToObj(this::generatePageViewStream)
                .flatMap(Function.identity());
        final var withFinalHeartbeat =
                Stream.concat(pageViewStream, Stream.of(new PageViewHeartbeat(totalEvents, Instant.MAX)));
        return withFinalHeartbeat.iterator();
    }

    private Stream<TimestampedUnion<PageView, PageViewHeartbeat>> generatePageViewStream(final long logicalTimestamp) {
        return UserIdHelper.getUserIds(totalUsers).stream()
                .map(userId -> new PageView(userId, logicalTimestamp));
    }

}
