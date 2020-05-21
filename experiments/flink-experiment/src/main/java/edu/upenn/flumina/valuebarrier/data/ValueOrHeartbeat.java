package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.Timestamped;

public interface ValueOrHeartbeat extends Timestamped {

    <T> T match(ValueCase<T> valueCase, HeartbeatVOHCase<T> heartbeatCase);

}
