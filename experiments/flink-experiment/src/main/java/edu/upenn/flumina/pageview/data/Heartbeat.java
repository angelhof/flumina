package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.TimestampedCore;

import java.util.function.Function;

public class Heartbeat extends TimestampedCore implements GetOrUpdateOrHeartbeat, PageViewOrHeartbeat {

    private static final long serialVersionUID = -5830590449039737456L;

    public Heartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public Heartbeat(final long logicalTimestamp, final long physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public String toString() {
        return "Heartbeat @ " + getLogicalTimestamp();
    }

    @Override
    public <T> T match(final Function<Get, T> getCase,
                       final Function<Update, T> updateCase,
                       final HeartbeatGUHCase<T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

    @Override
    public <T> T match(final Function<PageView, T> pageViewCase, final HeartbeatPVHCase<T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

}
