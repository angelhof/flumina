package edu.upenn.flumina.util;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ValueBarrierEncoder implements Encoder<Tuple2<Long, Long>> {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    @Override
    public void encode(Tuple2<Long, Long> tuple, OutputStream out) throws IOException {
        final StringBuilder sb = new StringBuilder();
        final LocalDateTime now = LocalDateTime.now();
        sb.append(now.format(formatter))
                .append(' ')
                .append(tuple.f0)
                .append(" @ ")
                .append(tuple.f1)
                .append('\n');
        out.write(sb.toString().getBytes());
    }
}
