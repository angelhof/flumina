package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Heartbeat;

import java.time.Instant;
import java.util.function.Function;

public class GetOrUpdateHeartbeat extends Heartbeat implements GetOrUpdateOrHeartbeat {

    private static final long serialVersionUID = -1206413969654177135L;

    // Default constructor so that the object is treated like POJO
    public GetOrUpdateHeartbeat() {
    }

    public GetOrUpdateHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public GetOrUpdateHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public <R> R match(final Function<GetOrUpdate, R> fstCase, final Function<GetOrUpdateHeartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
