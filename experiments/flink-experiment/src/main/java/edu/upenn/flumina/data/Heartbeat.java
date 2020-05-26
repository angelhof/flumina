package edu.upenn.flumina.data;

import java.time.Instant;

public class Heartbeat implements Timestamped {

    private static final long serialVersionUID = -8445219260831559864L;

    private final long logicalTimestamp;

    private Instant physicalTimestamp;
    private boolean hasPhysicalTimestamp;

    public Heartbeat(final long logicalTimestamp) {
        this.logicalTimestamp = logicalTimestamp;
        this.hasPhysicalTimestamp = false;
    }

    public Heartbeat(final long logicalTimestamp, final Instant physicalTimestamp) {
        this.logicalTimestamp = logicalTimestamp;
        this.physicalTimestamp = physicalTimestamp;
        this.hasPhysicalTimestamp = true;
    }

    @Override
    public long getLogicalTimestamp() {
        return logicalTimestamp;
    }

    @Override
    public Instant getPhysicalTimestamp() {
        return physicalTimestamp;
    }

    @Override
    public void setPhysicalTimestamp(final Instant physicalTimestamp) {
        this.physicalTimestamp = physicalTimestamp;
        this.hasPhysicalTimestamp = true;
    }

    @Override
    public boolean hasPhysicalTimestamp() {
        return hasPhysicalTimestamp;
    }

    @Override
    public String toString() {
        return "Heartbeat @ " + getLogicalTimestamp();
    }

}
