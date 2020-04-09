package edu.upenn.flumina.data.cases;

import edu.upenn.flumina.data.Timestamped;

public interface ValueOrHeartbeat extends Timestamped {

    <T> T match(ValueCase<T> valueCase, HeartbeatVOHCase<T> heartbeatCase);

}
