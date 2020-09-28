package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.TimestampedUnion;

import java.util.function.Function;

public interface BarrierOrHeartbeat extends TimestampedUnion<Barrier, BarrierHeartbeat> {

    <T> T match(Function<Barrier, T> barrierCase, Function<BarrierHeartbeat, T> heartbeatCase);

}
