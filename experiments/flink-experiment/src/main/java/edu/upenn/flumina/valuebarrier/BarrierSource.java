package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.source.GeneratorBasedSource;
import edu.upenn.flumina.valuebarrier.data.BarrierOrHeartbeat;

import java.time.Instant;

public class BarrierSource extends GeneratorBasedSource<BarrierOrHeartbeat> {

    private static final long serialVersionUID = 8077335205896599169L;

    public BarrierSource(final int totalValues,
                         final double valuesRate,
                         final int vbRatio,
                         final int hbRatio,
                         final Instant startTime) {
        super(new BarrierGenerator(totalValues, valuesRate, vbRatio, hbRatio), startTime);
    }

}
