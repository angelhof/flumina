package edu.upenn.flumina.valuebarrier;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static edu.upenn.flumina.time.TimeHelper.localFormat;

public class TimestampMapper implements MapFunction<Tuple3<Long, Long, Instant>, String> {

    private static final long serialVersionUID = 933001174516035217L;

    @Override
    public String map(final Tuple3<Long, Long, Instant> tuple) {
        final Instant now = Instant.now();
        // The field tuple.f2 corresponds to the physical timestamp
        final long latencyMillis = tuple.f2.until(now, ChronoUnit.MILLIS);
        return localFormat(now) +
                ' ' +
                tuple.f0 +
                " @ " +
                tuple.f1 +
                " [latency: " +
                latencyMillis +
                " ms]";
    }

}
