package edu.upenn.flumina.data.cases;

import edu.upenn.flumina.data.Barrier;

import java.util.function.Function;

@FunctionalInterface
public interface BarrierCase<T> extends Function<Barrier, T> {
}
