package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.source.GeneratorBasedSource;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;

import java.time.Instant;

public class ValueOrHeartbeatSource extends GeneratorBasedSource<ValueOrHeartbeat> {

    private static final long serialVersionUID = 6265081300394978260L;

    public ValueOrHeartbeatSource(final int totalValues, final double rate, final Instant startTime) {
        super(new ValueGenerator(totalValues, rate), startTime);
    }

}
