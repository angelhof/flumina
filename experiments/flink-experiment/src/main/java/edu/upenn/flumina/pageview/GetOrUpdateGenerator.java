package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.Union;
import edu.upenn.flumina.pageview.data.*;
import edu.upenn.flumina.source.GeneratorWithHeartbeats;

import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GetOrUpdateGenerator implements GeneratorWithHeartbeats<GetOrUpdate, Heartbeat> {

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
    public Iterator<Union<GetOrUpdate, Heartbeat>> getIterator() {
        final int total = totalEvents / 10;
        final var getOrUpdateStream = LongStream.range(0, total)
                .mapToObj(ts -> {
                    final var logicalTimestamp = ts * 10;
                    final var heartbeatStream =
                            Stream.of(new GetOrUpdateHeartbeat(logicalTimestamp));
                    final var getStream = (logicalTimestamp % 100 == 0) ?
                            generateGetStream(logicalTimestamp + 1) :
                            Stream.<Union<GetOrUpdate, Heartbeat>>empty();
                    final var updateStream = (logicalTimestamp % 1000 == 990) ?
                            generateUpdateStream(logicalTimestamp + 9) :
                            Stream.<Union<GetOrUpdate, Heartbeat>>empty();
                    return Stream.concat(Stream.concat(heartbeatStream, getStream), updateStream);
                })
                .flatMap(Function.identity());
        final var withFinalHeartbeat =
                Stream.concat(getOrUpdateStream, Stream.of(new GetOrUpdateHeartbeat(totalEvents, Long.MAX_VALUE)));
        return withFinalHeartbeat.iterator();
    }

    private Stream<Union<GetOrUpdate, Heartbeat>> generateGetStream(final long logicalTimestamp) {
        return UserIdHelper.getUserIds(totalUsers).stream()
                .map(userId -> new Get(userId, logicalTimestamp));
    }

    private Stream<Union<GetOrUpdate, Heartbeat>> generateUpdateStream(final long logicalTimestamp) {
        return UserIdHelper.getUserIds(totalUsers).stream().map(userId -> {
            final int zipCode = 10_000 + random.nextInt(90_000);
            return new Update(userId, zipCode, logicalTimestamp);
        });
    }

}
