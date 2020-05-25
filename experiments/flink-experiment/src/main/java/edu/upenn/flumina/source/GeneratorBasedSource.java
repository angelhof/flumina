package edu.upenn.flumina.source;

import edu.upenn.flumina.data.Timestamped;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;

import static edu.upenn.flumina.time.TimeHelper.localFormat;
import static edu.upenn.flumina.time.TimeHelper.millisSince;

public class GeneratorBasedSource<T extends Timestamped> extends RichParallelSourceFunction<T> implements Serializable {

    private static final long serialVersionUID = -6875008481095724331L;

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorBasedSource.class);

    private static final long SLEEP_GRANULARITY_MILLIS = 2;

    private volatile boolean isRunning = true;

    private final Generator<? extends T> generator;

    private final Instant startTime;

    /**
     * A parallel source that produces objects of type {@link T}. The objects are produced by {@code generator}
     * at a rate of {@code generator.getRate()} values per millisecond.
     * <p>
     * The source is initialized with {@code startTime}, a timestamp equivalent to the one that would be obtained
     * by running {@code System.nanoTime()}. This timestamp is used to synchronize multiple parallel instances
     * with the same starting time.
     *
     * @param generator The generator used by the source
     * @param startTime A timestamp equivalent to the one that would be obtained by running
     *                  {@code System.nanoTime()}
     */
    public GeneratorBasedSource(final Generator<? extends T> generator, final Instant startTime) {
        this.generator = generator;
        this.startTime = startTime;
    }

    @Override
    public void run(final SourceContext<T> ctx) {
        final double rate = generator.getRate();
        final Iterator<? extends T> iterator = generator.getIterator();
        T obj = iterator.next();

        // Future time is relative to startTime.
        if (LOG.isDebugEnabled()) {
            final Instant currentTime = Instant.now();
            LOG.debug("[{}] startTime = {} currentTime = {} diff = {} ms",
                    getRuntimeContext().getIndexOfThisSubtask(), localFormat(startTime), localFormat(currentTime),
                    startTime.until(currentTime, ChronoUnit.MILLIS));
        }
        do {
            final long iterationStartTime = millisSince(startTime);
            final long iterationStartTimeNormalized = (long) (iterationStartTime * rate);
            final long sleepAtLeastUntil = (long) ((iterationStartTime + SLEEP_GRANULARITY_MILLIS) * rate);
            LOG.trace("[{}] normalizedTime = {} sleepAtLeastUntil = {}",
                    getRuntimeContext().getIndexOfThisSubtask(), iterationStartTimeNormalized, sleepAtLeastUntil);

            // We're getting ready to sleep at least until sleepAtLeastUntil, so we first collect all objects
            // that should be collected prior to waking up.
            while (obj.getLogicalTimestamp() <= sleepAtLeastUntil) {
                if (!obj.hasPhysicalTimestamp()) {
                    obj.setPhysicalTimestamp(Instant.now());
                }
                ctx.collect(obj);
                if (iterator.hasNext()) {
                    obj = iterator.next();
                } else {
                    obj = null;
                    break;
                }
            }

            // We only sleep if there is still an object to be collected, and we sleep until it is
            // time to collect that object.
            if (obj != null) {
                try {
                    // At this point some time has passed while collecting objects. We need to make a correction and
                    // calculate the sleep time against current time instead of iterationStartTime.
                    final long currentTime = millisSince(startTime);

                    // In fact, the normalized current time may be way past obj.getTimestamp(), so we use Math.max.
                    // We max with 0 instead of SLEEP_GRANULARITY_MILLIS: if we are already past obj.getTimestamp(),
                    // then we have high congestion, and instead of actually sleeping for a couple of milliseconds,
                    // we yield the thread for a bit (Thread.sleep(0)) and get back ASAP to schedule more objects.
                    final long sleepTime = Math.max(0L, (long) ((obj.getLogicalTimestamp() / rate - currentTime)));
                    LOG.trace("[{}] Sleeping for {} ms", getRuntimeContext().getIndexOfThisSubtask(), sleepTime);
                    Thread.sleep(sleepTime);
                } catch (final InterruptedException e) {
                    // Flink guarantees to call cancel() before it interrupts any sleeping threads,
                    // so at this point we know we're done.
                }
            }
        } while (obj != null && isRunning);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
