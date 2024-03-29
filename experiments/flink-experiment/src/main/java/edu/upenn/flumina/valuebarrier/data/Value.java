package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.Heartbeat;

import java.util.function.Function;

public class Value extends Heartbeat implements ValueOrHeartbeat {

    private static final long serialVersionUID = -950069087838302251L;

    // All fields public so that the object is treated like POJO
    public long val;

    // Default constructor so that the object is treated like POJO
    public Value() {
    }

    public Value(final long val, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.val = val;
    }

    @Override
    public <T> T match(final Function<Value, T> valueCase, final Function<ValueHeartbeat, T> heartbeatCase) {
        return valueCase.apply(this);
    }

    @Override
    public String toString() {
        return "Value(" + val + ") @ " + getLogicalTimestamp();
    }

}
