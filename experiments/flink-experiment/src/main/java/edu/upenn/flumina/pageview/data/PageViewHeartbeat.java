package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.TimestampedUnion;

import java.util.function.Function;

public class PageViewHeartbeat extends Heartbeat implements TimestampedUnion<PageView, Heartbeat> {

    public PageViewHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public PageViewHeartbeat(final long logicalTimestamp, final long physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <R> R match(final Function<PageView, R> fstCase, final Function<Heartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
