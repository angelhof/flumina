package edu.upenn.flumina.data;

import java.io.Serializable;

public interface Timestamped extends Serializable {

    long getTimestamp();

}
