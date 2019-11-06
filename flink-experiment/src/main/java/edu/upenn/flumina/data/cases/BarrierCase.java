package edu.upenn.flumina.data.cases;

import java.util.function.Function;

@FunctionalInterface
public interface BarrierCase<T> extends Function<BarrierOrHeartbeat, T> {
}
