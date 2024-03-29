package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.Heartbeat;

import java.util.function.Function;

public class Get extends Heartbeat implements GetOrUpdate, GetOrUpdateOrHeartbeat {

    private static final long serialVersionUID = 4186323117708023380L;

    // All fields public so that the object is treated like POJO
    public int userId;

    // Default constructor so that the object is treated like POJO
    public Get() {
    }

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
