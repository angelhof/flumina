package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.Get;
import edu.upenn.flumina.pageview.data.GetOrUpdateHeartbeat;
import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.pageview.data.Update;
import edu.upenn.flumina.source.Generator;

import java.time.Instant;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GetOrUpdateGenerator implements Generator<GetOrUpdateOrHeartbeat> {

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
    public Iterator<GetOrUpdateOrHeartbeat> getIterator() {
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

    private Stream<GetOrUpdateOrHeartbeat> generateGetStream(final long logicalTimestamp) {
        return (logicalTimestamp % 1000 == 0) ?
                IntStream.range(0, totalUsers).mapToObj(userId -> new Get(userId, logicalTimestamp + 1)) :
                Stream.empty();
    }

    private Stream<GetOrUpdateOrHeartbeat> generateUpdateStream(final long logicalTimestamp) {
        return (logicalTimestamp % 10_000 == 9900) ?
                IntStream.range(0, totalUsers)
                        .mapToObj(userId -> new Update(userId, 10_000 + random.nextInt(90_000),
                                logicalTimestamp + 99)) :
                Stream.empty();
    }

}
