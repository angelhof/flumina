package edu.upenn.flumina.generator;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.Value;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;

import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ValueGenerator implements Generator<ValueOrHeartbeat> {

    private static final long serialVersionUID = -2469955428237914588L;

    private final int totalValues;
    private final double rate;

    public ValueGenerator(int totalValues, double rate) {
        this.totalValues = totalValues;
        this.rate = rate;
    }

    @Override
    public double getRate() {
        return rate;
    }

    @Override
    public Iterator<ValueOrHeartbeat> getIterator() {
        // Prepare a stream of Value objects with timestamps ranging from 1 to totalValues.
        // Add one heartbeat with timestamp (totalValues + 1) at the end.
        final Stream<Value> values = IntStream.rangeClosed(1, totalValues).mapToObj(t -> new Value(t, t));
        final Stream<ValueOrHeartbeat> withFinalHeartbeat =
                Stream.concat(values, Stream.of(new Heartbeat(totalValues + 1)));
        return withFinalHeartbeat.iterator();
    }
}
