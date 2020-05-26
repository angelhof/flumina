package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.TimestampedUnion;
import edu.upenn.flumina.pageview.data.Get;
import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.GetOrUpdateHeartbeat;
import edu.upenn.flumina.pageview.data.Update;
import edu.upenn.flumina.source.GeneratorWithHeartbeats;

import java.time.Instant;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GetOrUpdateGenerator implements GeneratorWithHeartbeats<GetOrUpdate, GetOrUpdateHeartbeat> {

    private static final long serialVersionUID = -8569916206087877650L;

    // TODO: Perhaps have a centralized random?
    private final Random random = new Random();

    private final int totalEvents;
    private final int totalUsers;
    private final double rate;

    public GetOrUpdateGenerator(final int totalEvents, final int totalUsers, final double rate) {
        this.totalEvents = totalEvents;
        this.totalUsers = totalUsers;
        this.rate = rate;
    }

    @Override
    public double getRate() {
        return rate;
    }

    @Override
    public Iterator<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>> getIterator() {
        final int total = totalEvents / 100;
        final var getOrUpdateStream = LongStream.range(0, total)
                .mapToObj(ts -> {
                    final var logicalTimestamp = ts * 100;
                    final var heartbeatStream =
                            Stream.of(new GetOrUpdateHeartbeat(logicalTimestamp));
                    final var getStream = (logicalTimestamp % 1000 == 0) ?
                            generateGetStream(logicalTimestamp + 1) :
                            Stream.<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>>empty();
                    final var updateStream = (logicalTimestamp % 10_000 == 9900) ?
                            generateUpdateStream(logicalTimestamp + 99) :
                            Stream.<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>>empty();
                    return Stream.concat(Stream.concat(heartbeatStream, getStream), updateStream);
                })
                .flatMap(Function.identity());
        final var withFinalHeartbeat =
                Stream.concat(getOrUpdateStream, Stream.of(new GetOrUpdateHeartbeat(totalEvents, Instant.MAX)));
        return withFinalHeartbeat.iterator();
    }

    private Stream<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>> generateGetStream(final long logicalTimestamp) {
        return UserIdHelper.getUserIds(totalUsers).stream()
                .map(userId -> new Get(userId, logicalTimestamp));
    }

    private Stream<TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat>> generateUpdateStream(final long logicalTimestamp) {
        return UserIdHelper.getUserIds(totalUsers).stream().map(userId -> {
            final int zipCode = 10_000 + random.nextInt(90_000);
            return new Update(userId, zipCode, logicalTimestamp);
        });
    }

}
