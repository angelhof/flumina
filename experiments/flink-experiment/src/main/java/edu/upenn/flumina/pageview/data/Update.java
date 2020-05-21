package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.TimestampedCore;

import java.util.function.Function;

public class Update extends TimestampedCore implements GetOrUpdateOrHeartbeat {

    private static final long serialVersionUID = -5650272144418361376L;

    private final int userId;
    private final int zipCode;

    public Update(final int userId, final int zipCode, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.userId = userId;
        this.zipCode = zipCode;
    }

    public int getUserId() {
        return userId;
    }

    public int getZipCode() {
        return zipCode;
    }

    @Override
    public <T> T match(final Function<Get, T> getCase,
                       final Function<Update, T> updateCase,
                       final HeartbeatGUHCase<T> heartbeatCase) {
        return updateCase.apply(this);
    }

}
