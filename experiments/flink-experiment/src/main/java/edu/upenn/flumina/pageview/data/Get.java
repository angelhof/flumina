package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Heartbeat;
import edu.upenn.flumina.data.TimestampedUnion;

import java.util.function.Function;

public class Get extends Heartbeat implements GetOrUpdate, TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat> {

    private static final long serialVersionUID = 4186323117708023380L;

    private final int userId;

    public Get(final int userId, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.userId = userId;
    }

    @Override
    public int getUserId() {
        return this.userId;
    }

    @Override
    public <R> R match(final GetCase<R> getCase, final UpdateCase<R> updateCase) {
        return getCase.apply(this);
    }

    @Override
    public <R> R match(final Function<GetOrUpdate, R> fstCase, final Function<GetOrUpdateHeartbeat, R> sndCase) {
        return fstCase.apply(this);
    }

    @Override
    public String toString() {
        return "Get{" +
                "userId=" + userId +
                "} @ " + getLogicalTimestamp();
    }

}