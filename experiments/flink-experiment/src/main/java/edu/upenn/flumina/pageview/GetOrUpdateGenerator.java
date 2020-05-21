package edu.upenn.flumina.pageview;

import edu.upenn.flumina.Generator;
import edu.upenn.flumina.pageview.data.Get;
import edu.upenn.flumina.pageview.data.GetOrUpdateOrHeartbeat;
import edu.upenn.flumina.pageview.data.Heartbeat;
import edu.upenn.flumina.pageview.data.Update;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class GetOrUpdateGenerator implements Generator<GetOrUpdateOrHeartbeat> {

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
        final Stream<GetOrUpdateOrHeartbeat> getOrUpdateStream = LongStream.range(0, totalEvents)
                .mapToObj(ts -> {
                    final int nextUserId = random.nextInt(totalUsers);
                    if (random.nextBoolean()) {
                        return new Get(nextUserId, ts);
                    } else {
                        final int nextZipCode = 10_000 + random.nextInt(90_000);
                        return new Update(nextUserId, nextZipCode, ts);
                    }
                });
        final Stream<GetOrUpdateOrHeartbeat> withFinalHeartbeat =
                Stream.concat(getOrUpdateStream, Stream.of(new Heartbeat(totalEvents, Long.MAX_VALUE)));
        return withFinalHeartbeat.iterator();
    }

}
