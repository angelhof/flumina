package edu.upenn.flumina.data.cases;

import edu.upenn.flumina.data.Heartbeat;

import java.util.function.Function;

@FunctionalInterface
public interface HeartbeatVOHCase<T> extends Function<Heartbeat, T> {
}
