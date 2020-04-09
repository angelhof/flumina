package edu.upenn.flumina;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class SleepExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(SleepExperiment.class);

    private static double getAvg(long[] samples) {
        return Arrays.stream(samples).average().getAsDouble();
    }

    public static void main(String[] args) throws Exception {
        // Running some experiments to measure the resolution of Thread.sleep()

        final long pauseInMicros = 10_000L;
        final long pauseInNanos = TimeUnit.MICROSECONDS.toNanos(pauseInMicros);
        final long millisPart = TimeUnit.MICROSECONDS.toMillis(pauseInMicros);
        final int nanosPart = Math.toIntExact(pauseInMicros - millisPart * 1000L) * 1000;
        final long minMillisPause = Math.max(millisPart, 1L);

        LOG.info("Sleeping for {} us = {} ms + {} ns, or {} ns", pauseInMicros, millisPart, nanosPart, pauseInNanos);

        final long[] samples = new long[1000];

        // Measure Thread.sleep(long)
        for (int i = 0; i < samples.length; i++) {
            long start = System.nanoTime();
            Thread.sleep(minMillisPause);
            samples[i] = System.nanoTime() - start;
        }
        LOG.info(String.format("Thread.sleep(long): avg = %.0f ns", getAvg(samples)));

        // Measure Thread.sleep(long, int)
        for (int i = 0; i < samples.length; i++) {
            long start = System.nanoTime();
            Thread.sleep(millisPart, nanosPart);
            samples[i] = System.nanoTime() - start;
        }
        LOG.info(String.format("Thread.sleep(long, int): avg = %.0f ns", getAvg(samples)));

        // Measure LockSupport.parkNanos(long)
        for (int i = 0; i < samples.length; i++) {
            long start = System.nanoTime();
            LockSupport.parkNanos(pauseInNanos);
            samples[i] = System.nanoTime() - start;
        }
        LOG.info(String.format("Thread.sleep(long): avg = %.0f ns", getAvg(samples)));
    }
}
