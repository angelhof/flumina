package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.Update;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimestampMapper implements MapFunction<Update, String> {

    private static final long serialVersionUID = -4405814449111392712L;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    @Override
    public String map(final Update update) {
        final StringBuilder sb = new StringBuilder();
        final long latencyMillis = (System.nanoTime() - update.getPhysicalTimestamp()) / 1_000_000;
        final LocalDateTime now = LocalDateTime.now();
        sb.append(now.format(TIMESTAMP_FORMATTER))
                .append(' ')
                .append(update.toString())
                .append(" [latency: ")
                .append(latencyMillis)
                .append(" ms]");
        return sb.toString();
    }

}
