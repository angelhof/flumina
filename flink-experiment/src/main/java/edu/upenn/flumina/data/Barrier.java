package edu.upenn.flumina.data;

import edu.upenn.flumina.data.cases.BarrierCase;
import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;
import edu.upenn.flumina.data.cases.HeartbeatBOHCase;

public class Barrier extends TimestampedCore implements BarrierOrHeartbeat {

    private static final long serialVersionUID = -168675275747391818L;

    public Barrier(long timestamp) {
        super(timestamp);
    }

    @Override
    public <T> T match(BarrierCase<T> barrierCase, HeartbeatBOHCase<T> heartbeatCase) {
        return barrierCase.apply(this);
    }
}
