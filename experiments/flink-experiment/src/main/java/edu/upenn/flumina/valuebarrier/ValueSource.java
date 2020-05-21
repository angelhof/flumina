package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.GeneratorBasedSource;
import edu.upenn.flumina.valuebarrier.data.ValueOrHeartbeat;

public class ValueSource extends GeneratorBasedSource<ValueOrHeartbeat> {

    private static final long serialVersionUID = 6265081300394978260L;

    public ValueSource(final int totalValues, final double rate, final long startTime) {
        super(new ValueGenerator(totalValues, rate), startTime);
    }

}
