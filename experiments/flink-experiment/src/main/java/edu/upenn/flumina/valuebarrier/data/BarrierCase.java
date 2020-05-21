package edu.upenn.flumina.valuebarrier.data;

import java.util.function.Function;

@FunctionalInterface
public interface BarrierCase<T> extends Function<Barrier, T> {

}
