package edu.upenn.flumina.data;

import edu.upenn.flumina.data.cases.*;

public class Heartbeat extends TimestampedCore implements BarrierOrHeartbeat, ValueOrHeartbeat {

    private static final long serialVersionUID = -5830590449039737456L;

    public Heartbeat(long timestamp) {
        super(timestamp);
    }

    @Override
    public <T> T match(BarrierCase<T> barrierCase, HeartbeatBOHCase<T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

    @Override
    public <T> T match(ValueCase<T> valueCase, HeartbeatVOHCase<T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

    @Override
    public String toString() {
        return "Heartbeat @ " + getTimestamp();
    }
}
