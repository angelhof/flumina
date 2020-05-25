package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.TimestampedCore;

import java.time.Instant;

public class Heartbeat extends TimestampedCore implements BarrierOrHeartbeat, ValueOrHeartbeat {

    private static final long serialVersionUID = -5830590449039737456L;

    public Heartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public Heartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public String toString() {
        return "Heartbeat @ " + getLogicalTimestamp();
    }

    @Override
    public <T> T match(final BarrierCase<T> barrierCase, final HeartbeatBOHCase<T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

    @Override
    public <T> T match(final ValueCase<T> valueCase, final HeartbeatVOHCase<T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

}
