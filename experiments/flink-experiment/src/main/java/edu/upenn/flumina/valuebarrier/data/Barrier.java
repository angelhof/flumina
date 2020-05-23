package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.TimestampedCore;

public class Barrier extends TimestampedCore implements BarrierOrHeartbeat {

    private static final long serialVersionUID = -168675275747391818L;

    public Barrier(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    @Override
    public <T> T match(final BarrierCase<T> barrierCase, final HeartbeatBOHCase<T> heartbeatCase) {
        return barrierCase.apply(this);
    }

    @Override
    public String toString() {
        return "Barrier @ " + getLogicalTimestamp();
    }

}
