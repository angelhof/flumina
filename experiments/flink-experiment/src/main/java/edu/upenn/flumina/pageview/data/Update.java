package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.TimestampedCore;
import edu.upenn.flumina.data.Union;

import java.util.function.Function;

public class Update extends TimestampedCore implements GetOrUpdate, Union<GetOrUpdate, Heartbeat> {

    private static final long serialVersionUID = -5650272144418361376L;

    private final int userId;
    private final int zipCode;

    public Update(final int userId, final int zipCode, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.userId = userId;
        this.zipCode = zipCode;
    }

    @Override
    public int getUserId() {
        return userId;
    }

    public int getZipCode() {
        return zipCode;
    }

    @Override
    public <R> R match(final GetCase<R> getCase, final UpdateCase<R> updateCase) {
        return updateCase.apply(this);
    }

    @Override
    public <R> R match(final Function<GetOrUpdate, R> fstCase, final Function<Heartbeat, R> sndCase) {
        return fstCase.apply(this);
    }

    @Override
    public String toString() {
        return "Update{" +
                "userId=" + userId +
                ", zipCode=" + zipCode +
                "} @ " + getLogicalTimestamp();
    }

}
