package edu.upenn.flumina.source;

import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;
import edu.upenn.flumina.generator.BarrierGenerator;

public class BarrierSource extends GeneratorBasedSource<BarrierOrHeartbeat> {

    private static final long serialVersionUID = 8077335205896599169L;

    public BarrierSource(int totalValues, double valuesRate, int vbRatio, int hbRatio, long startTime) {
        super(new BarrierGenerator(totalValues, valuesRate, vbRatio, hbRatio), startTime);
    }
}
