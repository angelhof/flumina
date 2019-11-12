package edu.upenn.flumina.data;

import edu.upenn.flumina.data.cases.BarrierCase;
import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;
import edu.upenn.flumina.data.cases.HeartbeatBOHCase;

import java.time.Instant;

public class Barrier extends TimestampedCore implements BarrierOrHeartbeat {

    private static final long serialVersionUID = -168675275747391818L;

    private final Instant latencyMarker = Instant.now();

    public Barrier(long timestamp) {
        super(timestamp);
    }

    @Override
    public <T> T match(BarrierCase<T> barrierCase, HeartbeatBOHCase<T> heartbeatCase) {
        return barrierCase.apply(this);
    }

    public Instant getLatencyMarker() {
        return latencyMarker;
    }

    @Override
    public String toString() {
        return "Barrier @ " + getTimestamp();
    }
}
