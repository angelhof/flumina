package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.Heartbeat;

import java.util.function.Function;

public class Barrier extends Heartbeat implements BarrierOrHeartbeat {

    private static final long serialVersionUID = -168675275747391818L;

    // Default constructor so that the object is treated like POJO
    public Barrier() {
    }

    public Barrier(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    @Override
    public <T> T match(final Function<Barrier, T> barrierCase, final Function<BarrierHeartbeat, T> heartbeatCase) {
        return barrierCase.apply(this);
    }

    @Override
    public String toString() {
        return "Barrier @ " + getLogicalTimestamp();
    }

}
