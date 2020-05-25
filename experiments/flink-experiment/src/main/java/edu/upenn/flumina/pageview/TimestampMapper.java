package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.Update;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static edu.upenn.flumina.time.TimeHelper.localFormat;

public class TimestampMapper implements MapFunction<Update, String> {

    private static final long serialVersionUID = -4405814449111392712L;

    @Override
    public String map(final Update update) {
        final Instant now = Instant.now();
        final long latencyMillis = update.getPhysicalTimestamp().until(now, ChronoUnit.MILLIS);
        return localFormat(now) +
                ' ' +
                update.toString() +
                " [latency: " +
                latencyMillis +
                " ms]";
    }

}
