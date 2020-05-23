package edu.upenn.flumina.source;

import edu.upenn.flumina.data.Timestamped;
import edu.upenn.flumina.data.Union;

public interface GeneratorWithHeartbeats<T extends Timestamped, H extends Timestamped> extends Generator<Union<T, H>> {

}
