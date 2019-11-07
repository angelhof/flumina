package edu.upenn.flumina.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimestampMapper implements MapFunction<Tuple2<Long, Long>, String> {

    private static final long serialVersionUID = 933001174516035217L;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    @Override
    public String map(Tuple2<Long, Long> tuple) {
        final StringBuilder sb = new StringBuilder();
        final LocalDateTime now = LocalDateTime.now();
        sb.append(now.format(TIMESTAMP_FORMATTER))
                .append(' ')
                .append(tuple.f0)
                .append(" @ ")
                .append(tuple.f1);
        return sb.toString();
    }
}
