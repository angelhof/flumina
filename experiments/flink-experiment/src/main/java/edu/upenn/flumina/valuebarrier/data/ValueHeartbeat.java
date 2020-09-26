package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.Heartbeat;

import java.time.Instant;
import java.util.function.Function;

public class ValueHeartbeat extends Heartbeat implements ValueOrHeartbeat {

    private static final long serialVersionUID = -4216851981696755420L;

    // Default constructor so that the object is treated like POJO
    ValueHeartbeat() {
    }

    public ValueHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public ValueHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <T> T match(final Function<Value, T> valueCase, final Function<ValueHeartbeat, T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

}
