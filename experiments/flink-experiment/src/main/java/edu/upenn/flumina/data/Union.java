package edu.upenn.flumina.data;

import java.util.function.Function;

public interface Union<S extends Timestamped, T extends Timestamped> extends Timestamped {

    <R> R match(Function<S, R> fstCase, Function<T, R> sndCase);

}
