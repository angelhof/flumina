package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Heartbeat;

import java.util.function.Function;

public class PageView extends Heartbeat implements PageViewOrHeartbeat {

    private static final long serialVersionUID = -3329652472136820306L;

    // All fields public so that the object is treated like POJO
    public int userId;

    // Default constructor so that the object is treated like POJO
    public PageView() {
    }

    public PageView(final int userId, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.userId = userId;
    }

    public int getUserId() {
        return userId;
    }

    @Override
    public <R> R match(final Function<PageView, R> fstCase, final Function<PageViewHeartbeat, R> sndCase) {
        return fstCase.apply(this);
    }

    @Override
    public String toString() {
        return "PageView{" +
                "userId=" + userId +
                "} @ " + getLogicalTimestamp();
    }

}
