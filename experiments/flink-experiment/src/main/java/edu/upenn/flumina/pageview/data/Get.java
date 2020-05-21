package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.TimestampedCore;

import java.util.function.Function;

public class Get extends TimestampedCore implements GetOrUpdateOrHeartbeat {

    private static final long serialVersionUID = 4186323117708023380L;

    private final int userId;

    public Get(final int userId, final long logicalTimestamp) {
        super(logicalTimestamp);
        this.userId = userId;
    }

    public int getUserId() {
        return this.userId;
    }

    @Override
    public <T> T match(final Function<Get, T> getCase,
                       final Function<Update, T> updateCase,
                       final HeartbeatGUHCase<T> heartbeatCase) {
        return getCase.apply(this);
    }

}
