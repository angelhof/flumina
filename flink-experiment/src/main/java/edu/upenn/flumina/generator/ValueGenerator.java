package edu.upenn.flumina.generator;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.Value;
import edu.upenn.flumina.data.cases.ValueOrHeartbeat;

import java.util.Iterator;
import java.util.stream.LongStream;
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
        // Prepare a stream of Value objects with timestamps ranging from 0 to totalValues-1.
        // Add one heartbeat with timestamp totalValues at the end.
        final Stream<Value> values = LongStream.range(0, totalValues).mapToObj(t -> new Value(t + 1, t));
        final Stream<ValueOrHeartbeat> withFinalHeartbeat =
                Stream.concat(values, Stream.of(new Heartbeat(totalValues)));
        return withFinalHeartbeat.iterator();
    }
}
