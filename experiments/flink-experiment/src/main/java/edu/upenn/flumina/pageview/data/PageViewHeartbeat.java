package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.TimestampedUnion;

import java.time.Instant;
import java.util.function.Function;

public class PageViewHeartbeat extends Heartbeat implements TimestampedUnion<PageView, PageViewHeartbeat> {

    private static final long serialVersionUID = -705084696418455909L;

    public PageViewHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public PageViewHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <R> R match(final Function<PageView, R> fstCase, final Function<PageViewHeartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
