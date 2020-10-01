package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Heartbeat;

import java.time.Instant;
import java.util.function.Function;

public class GetOrUpdateHeartbeat extends Heartbeat implements GetOrUpdateOrHeartbeat {

    private static final long serialVersionUID = -1206413969654177135L;

    private int userId;

    // Default constructor so that the object is treated like POJO
    public GetOrUpdateHeartbeat() {
    }

    public GetOrUpdateHeartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public GetOrUpdateHeartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    public GetOrUpdateHeartbeat(final GetOrUpdateHeartbeat hb, final int userId) {
        super(hb.logicalTimestamp, hb.physicalTimestamp);
        this.sourceIndex = hb.sourceIndex;
        this.userId = userId;
    }

    @Override
    public int getUserId() {
        return userId;
    }

    public void setUserId(final int userId) {
        this.userId = userId;
    }

    @Override
    public <R> R match(final Function<GetOrUpdate, R> fstCase, final Function<GetOrUpdateHeartbeat, R> sndCase) {
        return sndCase.apply(this);
    }

}
