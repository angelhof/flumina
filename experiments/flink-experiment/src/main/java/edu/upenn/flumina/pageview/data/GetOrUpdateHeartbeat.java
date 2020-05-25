package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.TimestampedUnion;

import java.time.Instant;
import java.util.function.Function;

public class GetOrUpdateHeartbeat extends Heartbeat implements TimestampedUnion<GetOrUpdate, Heartbeat> {

    public GetOrUpdateHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public GetOrUpdateHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <R> R match(final Function<GetOrUpdate, R> fstCase, final Function<Heartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
