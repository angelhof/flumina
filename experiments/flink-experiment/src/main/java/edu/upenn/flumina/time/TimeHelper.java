package edu.upenn.flumina.time;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TimeHelper {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    public static long millisSince(final Instant then) {
        return then.until(Instant.now(), ChronoUnit.MILLIS);
    }

    public static String localFormat(final Instant instant) {
        final LocalDateTime local = LocalDateTime.ofInstant(instant, Clock.systemDefaultZone().getZone());
        return local.format(TIMESTAMP_FORMATTER);
    }

    public static Instant max(final Instant fst, final Instant snd) {
        return fst.isAfter(snd) ? fst : snd;
    }

    public static Instant min(final Instant fst, final Instant snd) {
        return fst.isBefore(snd) ? fst : snd;
    }

}
