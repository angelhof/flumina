package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.Timestamped;

import java.util.function.Function;

public interface BarrierOrHeartbeat extends Timestamped {

    <T> T match(Function<Barrier, T> barrierCase, Function<BarrierHeartbeat, T> heartbeatCase);

}
