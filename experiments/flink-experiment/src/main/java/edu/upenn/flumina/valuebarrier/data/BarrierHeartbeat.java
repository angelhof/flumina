package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.Heartbeat;

import java.time.Instant;
import java.util.function.Function;

public class BarrierHeartbeat extends Heartbeat implements BarrierOrHeartbeat {

    private static final long serialVersionUID = -7970087496453091502L;

    // Default constructor so that the object is treated like POJO
    public BarrierHeartbeat() {
    }

    public BarrierHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public BarrierHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <T> T match(final Function<Barrier, T> barrierCase, final Function<BarrierHeartbeat, T> heartbeatCase) {
        return heartbeatCase.apply(this);
    }

}
