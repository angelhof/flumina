package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;
import edu.upenn.flumina.valuebarrier.data.Value;
import edu.upenn.flumina.valuebarrier.data.ValueHeartbeat;

import java.time.Instant;

public class ValueSource extends GeneratorWithHeartbeatsBasedSource<Value, ValueHeartbeat> {

    private static final long serialVersionUID = -6305125913806281941L;

    public ValueSource(final int totalValues, final double rate, final Instant startTime) {
        super(new ValueGenerator(totalValues, rate), startTime);
    }

}
