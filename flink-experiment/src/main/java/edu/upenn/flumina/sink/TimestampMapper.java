package edu.upenn.flumina.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimestampMapper implements MapFunction<Tuple3<Long, Long, Instant>, String> {

    private static final long serialVersionUID = 933001174516035217L;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    @Override
    public String map(Tuple3<Long, Long, Instant> tuple) {
        final StringBuilder sb = new StringBuilder();
        final Instant now = Instant.now();
        final LocalDateTime ldt = LocalDateTime.ofInstant(now, ZoneId.systemDefault());
        final Duration latency = Duration.between(tuple.f2, now);
        sb.append(ldt.format(TIMESTAMP_FORMATTER))
                .append(' ')
                .append(tuple.f0)
                .append(" @ ")
                .append(tuple.f1)
                .append(" [latency: ")
                .append(latency.toMillis())
                .append(" ms]");
        return sb.toString();
    }
}
