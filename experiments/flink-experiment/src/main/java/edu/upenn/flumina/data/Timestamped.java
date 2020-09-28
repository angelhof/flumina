package edu.upenn.flumina.data;

import java.io.Serializable;
import java.time.Instant;

public interface Timestamped extends Serializable {

    long getLogicalTimestamp();

    Instant getPhysicalTimestamp();

    void setPhysicalTimestamp(Instant physicalTimestamp);

    boolean hasPhysicalTimestamp();

    /**
     * The index of the parallel subtask that produced this event.
     *
     * @return Index
     */
    int getSourceIndex();

    void setSourceIndex(int sourceIndex);

}
