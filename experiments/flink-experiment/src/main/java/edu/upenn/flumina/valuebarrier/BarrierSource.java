package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.GeneratorBasedSource;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;

public class BarrierSource extends GeneratorBasedSource<BarrierOrHeartbeat> {

    private static final long serialVersionUID = 8077335205896599169L;

    public BarrierSource(final int totalValues,
                         final double valuesRate,
                         final int vbRatio,
                         final int hbRatio,
                         final long startTime) {
        super(new BarrierGenerator(totalValues, valuesRate, vbRatio, hbRatio), startTime);
    }

}
