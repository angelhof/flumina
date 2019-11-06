package edu.upenn.flumina.data.cases;

import java.util.function.Function;

@FunctionalInterface
public interface ValueCase<T> extends Function<ValueOrHeartbeat, T> {
}
