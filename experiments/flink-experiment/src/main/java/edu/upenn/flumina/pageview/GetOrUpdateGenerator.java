package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.TimestampedUnion;
import edu.upenn.flumina.pageview.data.Get;
import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.GetOrUpdateHeartbeat;
import edu.upenn.flumina.pageview.data.Update;
import edu.upenn.flumina.source.Generator;

import java.time.Instant;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GetOrUpdateGenerator implements Generator<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>> {

    private static final long serialVersionUID = -8569916206087877650L;

    // TODO: Perhaps have a centralized random?
    private final Random random = new Random();

    private final int totalEvents;
    private final double rate;
    private int userId;

    public GetOrUpdateGenerator(final int totalEvents, final double rate) {
        this.totalEvents = totalEvents;
        this.rate = rate;
    }

    @Override
    public double getRate() {
        return rate;
    }

    /**
     * Instead of setting {@code userId} in {@link GetOrUpdateGenerator#GetOrUpdateGenerator(int, double)},
     * we provide a setter method. This is because we want user IDs to be indices of parallel
     * source instances, and the indices are only known after the generator is constructed.
     *
     * <p>It is crucial that {@link GetOrUpdateGenerator#getIterator()} is called <b>after</b> the user ID is set.</p>
     *
     * @param userId User ID
     */
    public void setUserId(final int userId) {
        this.userId = userId;
    }

    @Override
    public Iterator<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>> getIterator() {
        final int total = totalEvents / 100;
        final var getOrUpdateStream = LongStream.range(0, total)
                .mapToObj(ts -> {
                    final var logicalTimestamp = ts * 100;
                    final var heartbeatStream =
                            Stream.of(new GetOrUpdateHeartbeat(logicalTimestamp));
                    final var getStream = generateGetStream(logicalTimestamp);
                    final var updateStream = generateUpdateStream(logicalTimestamp);
                    return Stream.concat(Stream.concat(heartbeatStream, getStream), updateStream);
                })
                .flatMap(Function.identity());
        final var withFinalHeartbeat =
                Stream.concat(getOrUpdateStream, Stream.of(new GetOrUpdateHeartbeat(totalEvents, Instant.MAX)));
        return withFinalHeartbeat.iterator();
    }

    private Stream<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>> generateGetStream(final long logicalTimestamp) {
        return (logicalTimestamp % 1000 == 0) ?
                Stream.of(new Get(userId, logicalTimestamp + 1)) :
                Stream.empty();
    }

    private Stream<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>> generateUpdateStream(final long logicalTimestamp) {
        return (logicalTimestamp % 10_000 == 9900) ?
                Stream.of(new Update(userId, 10_000 + random.nextInt(90_000), logicalTimestamp + 99)) :
                Stream.empty();
    }

}
