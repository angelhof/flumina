package edu.upenn.flumina.data;

import edu.upenn.flumina.data.cases.HeartbeatVOHCase;
import edu.upenn.flumina.data.cases.ValueCase;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;

public class Value extends TimestampedCore implements ValueOrHeartbeat {

    private static final long serialVersionUID = -950069087838302251L;

    private final long val;

    public Value(long val, long timestamp) {
        super(timestamp);
        this.val = val;
    }

    public long getVal() {
        return val;
    }

    @Override
    public <T> T match(ValueCase<T> valueCase, HeartbeatVOHCase<T> heartbeatCase) {
        return valueCase.apply(this);
    }

    @Override
    public String toString() {
        return "Value(" + getVal() + ") @ " + getTimestamp();
    }
}
