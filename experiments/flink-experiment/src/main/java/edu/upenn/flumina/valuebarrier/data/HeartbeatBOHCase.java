package edu.upenn.flumina.valuebarrier.data;

import java.util.function.Function;

@FunctionalInterface
public interface HeartbeatBOHCase<T> extends Function<Heartbeat, T> {

}
