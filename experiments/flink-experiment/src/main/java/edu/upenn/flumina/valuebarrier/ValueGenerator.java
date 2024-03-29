package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.source.Generator;
import edu.upenn.flumina.valuebarrier.data.Value;
import edu.upenn.flumina.valuebarrier.data.ValueHeartbeat;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;

import java.time.Instant;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ValueGenerator implements Generator<ValueOrHeartbeat> {

    private static final long serialVersionUID = -2469955428237914588L;

    private final int totalValues;
    private final double rate;

    public ValueGenerator(final int totalValues, final double rate) {
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
        final var values = LongStream.range(0, totalValues).<ValueOrHeartbeat>mapToObj(t -> new Value(t + 1, t));
        final var withFinalHeartbeat =
                Stream.concat(values, Stream.of(new ValueHeartbeat(totalValues, Instant.MAX)));
        return withFinalHeartbeat.iterator();
    }

}
