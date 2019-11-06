package edu.upenn.flumina.data;

public class TimestampedCore implements Timestamped {

    private static final long serialVersionUID = -8445219260831559864L;

    private final long timestamp;

    public TimestampedCore(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }
}
