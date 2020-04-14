package edu.upenn.flumina.data;

import java.io.Serializable;

public interface Timestamped extends Serializable {

    long getLogicalTimestamp();

    long getPhysicalTimestamp();

    void setPhysicalTimestamp(long physicalTimestamp);

    boolean hasPhysicalTimestamp();

}
