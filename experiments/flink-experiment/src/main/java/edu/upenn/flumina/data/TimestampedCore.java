package edu.upenn.flumina.data;

public class TimestampedCore implements Timestamped {

    private static final long serialVersionUID = -8445219260831559864L;

    private final long logicalTimestamp;

    private long physicalTimestamp;
    private boolean hasPhysicalTimestamp;

    public TimestampedCore(long logicalTimestamp) {
        this.logicalTimestamp = logicalTimestamp;
        this.hasPhysicalTimestamp = false;
    }

    public TimestampedCore(long logicalTimestamp, long physicalTimestamp) {
        this.logicalTimestamp = logicalTimestamp;
        this.physicalTimestamp = physicalTimestamp;
        this.hasPhysicalTimestamp = true;
    }

    @Override
    public long getLogicalTimestamp() {
        return logicalTimestamp;
    }

    @Override
    public long getPhysicalTimestamp() {
        return physicalTimestamp;
    }

    @Override
    public void setPhysicalTimestamp(long physicalTimestamp) {
        this.physicalTimestamp = physicalTimestamp;
        this.hasPhysicalTimestamp = true;
    }

    @Override
    public boolean hasPhysicalTimestamp() {
        return hasPhysicalTimestamp;
    }
}
