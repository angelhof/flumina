package edu.upenn.flumina.pageview.data;

import java.util.function.Function;

@FunctionalInterface
public interface HeartbeatPVHCase<T> extends Function<Heartbeat, T> {

}
