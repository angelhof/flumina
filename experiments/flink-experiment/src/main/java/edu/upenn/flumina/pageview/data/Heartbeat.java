package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.TimestampedCore;

public class Heartbeat extends TimestampedCore {

    private static final long serialVersionUID = -5830590449039737456L;

    public Heartbeat(final long logicalTimestamp) {
        super(logicalTimestamp);
    }

    public Heartbeat(final long logicalTimestamp, final long physicalTimestamp) {
        super(logicalTimestamp, physicalTimestamp);
    }

    @Override
    public String toString() {
        return "Heartbeat @ " + getLogicalTimestamp();
    }

}
