package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.Timestamped;

import java.util.function.Function;

public interface ValueOrHeartbeat extends Timestamped {

    <T> T match(Function<Value, T> valueCase, Function<ValueHeartbeat, T> heartbeatCase);

}
