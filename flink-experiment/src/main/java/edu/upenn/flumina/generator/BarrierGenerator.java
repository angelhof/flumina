package edu.upenn.flumina.generator;

import edu.upenn.flumina.data.Barrier;
import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.cases.BarrierOrHeartbeat;

import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class BarrierGenerator implements Generator<BarrierOrHeartbeat> {

    private static final long serialVersionUID = -3220535123134655466L;

    private final int totalValues;
    private final double valuesRate;
    private final int vbRatio;
    private final int hbRatio;

    public BarrierGenerator(int totalValues, double valuesRate, int vbRatio, int hbRatio) {
        this.totalValues = totalValues;
        this.valuesRate = valuesRate;
        this.vbRatio = vbRatio;
        this.hbRatio = hbRatio;
    }

    @Override
    public double getRate() {
        return valuesRate;
    }

    @Override
    public Iterator<BarrierOrHeartbeat> getIterator() {
        // There should be one barrier for every batch of (totalValues / vbRatio) values.
        // Furthermore, for every barrier there are (hbRatio-1) heartbeats, i.e., for every
        // barrier there are hbRatio barriers or heartbeats.
        //
        // For example, let totalValues = 100, vbRatio = 10, hbRatio = 5. There should
        // be 100 / 10 = 10 barriers, and 10 * 5 = 50 barriers and heartbeats.
        // The stream with timestamps should be as follows:
        //
        //   hb(1), hb(3), hb(5), hb(7), barrier(9),
        //     ...,
        //       hb(91), hb(93), hb(95), hb(97), barrier(99)
        //
        // We add one heartbeat with timestamp totalValues at the end.
        final int totalBarrierOrHeartbeats = totalValues / vbRatio * hbRatio;
        final Stream<BarrierOrHeartbeat> barriers = LongStream.rangeClosed(1, totalBarrierOrHeartbeats)
                .mapToObj(i -> {
                    if (i % hbRatio == 0) {
                        // Multiply vbRatio / hbRatio first to avoid potential overflow.
                        // Assumes that hbRatio divides vbRatio.
                        return new Barrier(i * (vbRatio / hbRatio) - 1);
                    } else {
                        return new Heartbeat(i * (vbRatio / hbRatio) - 1);
                    }
                });
        final Stream<BarrierOrHeartbeat> withFinalHeartbeat =
                Stream.concat(barriers, Stream.of(new Heartbeat(totalValues)));
        return withFinalHeartbeat.iterator();
    }
}
