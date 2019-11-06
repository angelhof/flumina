package edu.upenn.flumina.data.cases;

import java.util.function.Function;

@FunctionalInterface
public interface HeartbeatVOHCase<T> extends Function<ValueOrHeartbeat, T> {
}
