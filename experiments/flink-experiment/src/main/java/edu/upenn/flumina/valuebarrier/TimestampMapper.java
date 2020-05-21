package edu.upenn.flumina.valuebarrier;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimestampMapper implements MapFunction<Tuple3<Long, Long, Long>, String> {

    private static final long serialVersionUID = 933001174516035217L;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    @Override
    public String map(final Tuple3<Long, Long, Long> tuple) {
        final StringBuilder sb = new StringBuilder();
        // The field tuple.f2 corresponds to the physical timestamp
        final long latencyMillis = (System.nanoTime() - tuple.f2) / 1_000_000;
        final LocalDateTime now = LocalDateTime.now();
        sb.append(now.format(TIMESTAMP_FORMATTER))
                .append(' ')
                .append(tuple.f0)
                .append(" @ ")
                .append(tuple.f1)
                .append(" [latency: ")
                .append(latencyMillis)
                .append(" ms]");
        return sb.toString();
    }

}
