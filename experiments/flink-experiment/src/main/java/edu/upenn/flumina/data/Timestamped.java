package edu.upenn.flumina.data;

import java.io.Serializable;
import java.time.Instant;

public interface Timestamped extends Serializable {

    long getLogicalTimestamp();

    Instant getPhysicalTimestamp();

    void setPhysicalTimestamp(Instant physicalTimestamp);

    boolean hasPhysicalTimestamp();

}
