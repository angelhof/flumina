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

    /**
     * Calling {@code toEpochMilli} on {@code Instant.MAX} causes long overflow,
     * so this method is a workaround.
     *
     * @param instant Instant which we want to convert to milliseconds since the epoch
     * @return {@code instant.toEpochMilli()} or {@code Long.MAX_VALUE} if {@code instant} is {@code Instant.MAX}
     */
    public static long toEpochMilli(final Instant instant) {
        if (instant.equals(Instant.MAX)) {
            return Long.MAX_VALUE;
        }
        return instant.toEpochMilli();
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
