package edu.upenn.flumina.data.cases;

import edu.upenn.flumina.data.Heartbeat;

import java.util.function.Function;

@FunctionalInterface
public interface HeartbeatBOHCase<T> extends Function<Heartbeat, T> {
}
