package edu.upenn.flumina.source;

import edu.upenn.flumina.data.Timestamped;
import edu.upenn.flumina.generator.Generator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Iterator;

public class GeneratorBasedSource<T extends Timestamped> extends RichParallelSourceFunction<T> implements Serializable {

    private static final long serialVersionUID = -6875008481095724331L;

    private static final Logger LOG = LogManager.getLogger();

    private static final long SLEEP_GRANULARITY_MILLIS = 2;

    private volatile boolean isRunning = true;

    private final Generator<? extends T> generator;

    /**
     * A parallel source that produces objects of type {@link T}. The objects are produced by {@code generator}
     * at a rate of {@code generator.getRate()} values per millisecond.
     *
     * @param generator The generator used by the source
     */
    public GeneratorBasedSource(Generator<? extends T> generator) {
        this.generator = generator;
    }

    @Override
    public void run(SourceContext<T> ctx) {
        final double rate = generator.getRate();
        final Iterator<? extends T> iterator = generator.getIterator();
        T obj = iterator.next();

        // Future time is relative to startTime.
        final long startTime = System.nanoTime();
        do {
            final double iterationStartTime = (System.nanoTime() - startTime) / 1_000_000.0;
            final long iterationStartTimeNormalized = (long) (iterationStartTime * rate);
            final long sleepAtLeastUntil = (long) ((iterationStartTime + SLEEP_GRANULARITY_MILLIS) * rate);
            LOG.debug("[{}] normalizedTime = {} sleepAtLeastUntil = {}",
                    getRuntimeContext().getIndexOfThisSubtask(), iterationStartTimeNormalized, sleepAtLeastUntil);

            // We're getting ready to sleep at least until sleepAtLeastUntil, so we first collect all objects
            // that should be collected prior to waking up.
            while (obj.getTimestamp() <= sleepAtLeastUntil) {
//                ctx.collectWithTimestamp(obj, obj.getTimestamp());
                ctx.collect(obj);
                if (iterator.hasNext()) {
                    obj = iterator.next();
                } else {
                    obj = null;
                    break;
                }
            }
//            ctx.emitWatermark(new Watermark(sleepAtLeastUntil));
//            ctx.markAsTemporarilyIdle();

            // We only sleep if there still is an object to be collected, and we sleep until it is
            // time to collect that object.
            if (obj != null) {
                try {
                    // At this point some time has passed while collecting objects. We need to make a correction and
                    // calculate the sleep time against current time instead of iterationStartTime.
                    final double currentTime = (System.nanoTime() - startTime) / 1_000_000.0;
                    // In fact, the normalized current time may be way past obj.getTimestamp(), so we use Math.max:
                    final long sleepTime =
                            Math.max(SLEEP_GRANULARITY_MILLIS, (long) ((obj.getTimestamp() / rate - currentTime)));
                    LOG.debug("[{}] Sleeping for {} ms", getRuntimeContext().getIndexOfThisSubtask(), sleepTime);
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
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
