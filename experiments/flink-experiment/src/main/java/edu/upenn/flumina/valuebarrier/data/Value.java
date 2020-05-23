package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.TimestampedCore;

public class Value extends TimestampedCore implements ValueOrHeartbeat {

    private static final long serialVersionUID = -950069087838302251L;

    private final long val;

    public Value(final long val, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.val = val;
    }

    public long getVal() {
        return val;
    }

    @Override
    public <T> T match(final ValueCase<T> valueCase, final HeartbeatVOHCase<T> heartbeatCase) {
        return valueCase.apply(this);
    }

    @Override
    public String toString() {
        return "Value(" + getVal() + ") @ " + getLogicalTimestamp();
    }

}
