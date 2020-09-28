package edu.upenn.flumina.valuebarrier.data;

import edu.upenn.flumina.data.TimestampedUnion;

import java.util.function.Function;

public interface ValueOrHeartbeat extends TimestampedUnion<Value, ValueHeartbeat> {

    <T> T match(Function<Value, T> valueCase, Function<ValueHeartbeat, T> heartbeatCase);

}
