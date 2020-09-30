package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.PageView;
import edu.upenn.flumina.pageview.data.PageViewHeartbeat;
import edu.upenn.flumina.pageview.data.PageViewOrHeartbeat;
import edu.upenn.flumina.source.Generator;

import java.time.Instant;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class PageViewGenerator implements Generator<PageViewOrHeartbeat> {

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
    public Iterator<PageViewOrHeartbeat> getIterator() {
        final var pageViewStream = LongStream.range(0, totalEvents)
                .mapToObj(this::generatePageViewStream)
                .flatMap(Function.identity());
        final var withFinalHeartbeats =
                Stream.concat(pageViewStream, generateHeartbeatsStream());
        return withFinalHeartbeats.iterator();
    }

    private Stream<PageViewOrHeartbeat> generatePageViewStream(final long logicalTimestamp) {
        return IntStream.range(0, totalUsers)
                .mapToObj(userId -> new PageView(userId, logicalTimestamp));
    }

    private Stream<PageViewOrHeartbeat> generateHeartbeatsStream() {
        return IntStream.range(0, totalUsers)
                .mapToObj(userId -> new PageViewHeartbeat(userId, totalEvents, Instant.MAX));
    }

}
