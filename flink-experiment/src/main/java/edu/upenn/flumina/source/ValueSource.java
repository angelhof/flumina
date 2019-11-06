package edu.upenn.flumina.source;

import edu.upenn.flumina.data.cases.ValueOrHeartbeat;
import edu.upenn.flumina.generator.ValueGenerator;

public class ValueSource extends GeneratorBasedSource<ValueOrHeartbeat> {

    private static final long serialVersionUID = 6265081300394978260L;

    public ValueSource(int totalValues, double rate) {
        super(new ValueGenerator(totalValues, rate));
    }
}
