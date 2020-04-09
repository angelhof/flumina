package edu.upenn.flumina.data.cases;

import edu.upenn.flumina.data.Value;

import java.util.function.Function;

@FunctionalInterface
public interface ValueCase<T> extends Function<Value, T> {
}
