package edu.upenn.flumina.valuebarrier;

import edu.upenn.flumina.source.GeneratorWithHeartbeatsBasedSource;
import edu.upenn.flumina.valuebarrier.data.Barrier;
import edu.upenn.flumina.valuebarrier.data.BarrierHeartbeat;

import java.time.Instant;

public class BarrierSource extends GeneratorWithHeartbeatsBasedSource<Barrier, BarrierHeartbeat> {

    private static final long serialVersionUID = 5539974030282168452L;

    public BarrierSource(final int totalValues,
                         final double valuesRate,
                         final int vbRatio,
                         final int hbRatio,
                         final Instant startTime) {
        super(new BarrierGenerator(totalValues, valuesRate, vbRatio, hbRatio), startTime);
    }

}
