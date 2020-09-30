package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Heartbeat;

import java.time.Instant;
import java.util.function.Function;

public class PageViewHeartbeat extends Heartbeat implements PageViewOrHeartbeat {

    private static final long serialVersionUID = -705084696418455909L;

    public int userId;

    // Default constructor so that the object is treated like POJO
    public PageViewHeartbeat() {
    }

    public PageViewHeartbeat(final int userId, final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
        this.userId = userId;
    }

    @Override
    public int getUserId() {
        return userId;
    }

    @Override
    public <R> R match(final Function<PageView, R> fstCase, final Function<PageViewHeartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
