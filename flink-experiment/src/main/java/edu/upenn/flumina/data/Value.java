package edu.upenn.flumina.data;

import edu.upenn.flumina.data.cases.HeartbeatVOHCase;
import edu.upenn.flumina.data.cases.ValueCase;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;

public class Value extends TimestampedCore implements ValueOrHeartbeat {

    private static final long serialVersionUID = -950069087838302251L;

    private final int val;

    public Value(int val, long timestamp) {
        super(timestamp);
        this.val = val;
    }

    public int getVal() {
        return val;
    }

    @Override
    public <T> T match(ValueCase<T> valueCase, HeartbeatVOHCase<T> heartbeatCase) {
        return valueCase.apply(this);
    }
}
