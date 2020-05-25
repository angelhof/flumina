package edu.upenn.flumina.source;

import edu.upenn.flumina.data.Timestamped;
import edu.upenn.flumina.data.TimestampedUnion;

public interface GeneratorWithHeartbeats<T extends Timestamped, H extends Timestamped> extends Generator<TimestampedUnion<T, H>> {

}
