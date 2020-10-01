package edu.upenn.flumina.util;

import edu.upenn.flumina.data.Timestamped;

import java.io.Serializable;
import java.util.Comparator;

public class TimestampComparator implements Comparator<Timestamped>, Serializable {

    @Override
    public int compare(final Timestamped t1, final Timestamped t2) {
        return t1.getPhysicalTimestamp().compareTo(t2.getPhysicalTimestamp());
    }

}
