package edu.upenn.flumina.data.cases;

import edu.upenn.flumina.data.Timestamped;

public interface BarrierOrHeartbeat extends Timestamped {

    <T> T match(BarrierCase<T> barrierCase, HeartbeatBOHCase<T> heartbeatCase);

}
