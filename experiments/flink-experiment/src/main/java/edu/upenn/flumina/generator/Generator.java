package edu.upenn.flumina.generator;

import java.io.Serializable;
import java.util.Iterator;

/**
 * The generator that produces objects of type {@link T}. The generator should initialize
 * a new "batch" on each invocation of {@link Generator#getIterator()} and return an iterator over the batch.
 * The batch should contain at least one object.
 *
 * @param <T> Type of the produced objects
 */
public interface Generator<T> extends Serializable {

    double getRate();

    Iterator<T> getIterator();
}
