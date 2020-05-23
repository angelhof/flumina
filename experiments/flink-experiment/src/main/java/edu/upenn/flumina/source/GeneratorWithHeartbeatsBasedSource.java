package edu.upenn.flumina.source;

import edu.upenn.flumina.data.Timestamped;
import edu.upenn.flumina.data.Union;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class GeneratorWithHeartbeatsBasedSource<T extends Timestamped, H extends Timestamped>
        extends RichParallelSourceFunction<T> implements Serializable {

    private static final long serialVersionUID = -3245902708095159178L;

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorWithHeartbeatsBasedSource.class);

    private static final long SLEEP_GRANULARITY_MILLIS = 2;

    private volatile boolean isRunning = true;

    private final GeneratorWithHeartbeats<T, H> generator;

    private final long startTime;

    public GeneratorWithHeartbeatsBasedSource(final GeneratorWithHeartbeats<T, H> generator,
                                              final long startTime) {
        this.generator = generator;
        this.startTime = startTime;
    }

    @Override
    public void run(final SourceContext<T> ctx) {
        final double rate = generator.getRate();
        final Iterator<Union<T, H>> iterator = generator.getIterator();
        Union<T, H> obj = iterator.next();

        // Future time is relative to startTime.
        if (LOG.isDebugEnabled()) {
            final long systemNanoTime = System.nanoTime();
            LOG.debug("[{}] startTime = {} System.nanoTime() = {} diff = {} ms",
                    getRuntimeContext().getIndexOfThisSubtask(), startTime, systemNanoTime,
                    TimeUnit.NANOSECONDS.toMillis(systemNanoTime - startTime));
        }
        do {
            final double iterationStartTime = (System.nanoTime() - startTime) / 1_000_000.0;
            final long iterationStartTimeNormalized = (long) (iterationStartTime * rate);
            final long sleepAtLeastUntil = (long) ((iterationStartTime + SLEEP_GRANULARITY_MILLIS) * rate);
            LOG.trace("[{}] normalizedTime = {} sleepAtLeastUntil = {}",
                    getRuntimeContext().getIndexOfThisSubtask(), iterationStartTimeNormalized, sleepAtLeastUntil);

            // We're getting ready to sleep at least until sleepAtLeastUntil, so we first collect all objects
            // that should be collected prior to waking up.
            while (obj.getLogicalTimestamp() <= sleepAtLeastUntil) {
                final long physicalTimestamp;
                if (obj.hasPhysicalTimestamp()) {
                    physicalTimestamp = obj.getPhysicalTimestamp();
                } else {
                    physicalTimestamp = System.nanoTime();
                    obj.setPhysicalTimestamp(physicalTimestamp);
                }
                obj.match(
                        event -> {
                            ctx.collectWithTimestamp(event, physicalTimestamp);
                            return null;
                        },
                        heartbeat -> {
                            ctx.emitWatermark(new Watermark(physicalTimestamp));
                            return null;
                        }
                );
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
                    final double currentTime = (System.nanoTime() - startTime) / 1_000_000.0;

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
