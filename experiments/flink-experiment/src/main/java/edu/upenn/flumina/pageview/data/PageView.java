package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.TimestampedCore;

import java.util.function.Function;

public class PageView extends TimestampedCore implements PageViewOrHeartbeat {

    private static final long serialVersionUID = -3329652472136820306L;

    private final int userId;

    public PageView(final int userId, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.userId = userId;
    }

    public int getUserId() {
        return userId;
    }

    @Override
    public <T> T match(final Function<PageView, T> pageViewCase, final HeartbeatPVHCase<T> heartbeatCase) {
        return pageViewCase.apply(this);
    }

}
