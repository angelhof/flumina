package edu.upenn.flumina.data.cases;

import java.util.function.Function;

@FunctionalInterface
public interface HeartbeatBOHCase<T> extends Function<BarrierOrHeartbeat, T> {
}
