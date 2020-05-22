package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.Union;
import edu.upenn.flumina.pageview.data.*;
import edu.upenn.flumina.source.GeneratorWithHeartbeats;

import java.util.Iterator;
import java.util.Random;
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
        final Stream<Union<GetOrUpdate, Heartbeat>> getOrUpdateStream = LongStream.range(0, totalEvents)
                .mapToObj(ts -> {
                    final int nextUserId = random.nextInt(totalUsers);
                    if (random.nextBoolean()) {
                        return new Get(nextUserId, ts);
                    } else {
                        final int nextZipCode = 10_000 + random.nextInt(90_000);
                        return new Update(nextUserId, nextZipCode, ts);
                    }
                });
        final Stream<Union<GetOrUpdate, Heartbeat>> withFinalHeartbeat =
                Stream.concat(getOrUpdateStream, Stream.of(new GetOrUpdateHeartbeat(totalEvents, Long.MAX_VALUE)));
        return withFinalHeartbeat.iterator();
    }

}
