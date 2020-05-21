package edu.upenn.flumina.valuebarrier.data;

import java.util.function.Function;

@FunctionalInterface
public interface ValueCase<T> extends Function<Value, T> {

}
