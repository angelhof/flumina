package edu.upenn.flumina.util;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FlinkHashInverterTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkHashInverterTest.class);

    @Test
    public void testGetMapping() {
        final int totalKeys = 100;
        final List<Integer> mapping = FlinkHashInverter.getMapping(totalKeys);
        assertEquals("Mapping should have correct size", totalKeys, mapping.size());
        for (int i = 0; i < totalKeys; ++i) {
            final int obtained = KeyGroupRangeAssignment.assignKeyToParallelOperator(mapping.get(i),
                    KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM, totalKeys);
            assertEquals("Key " + i + " should map to " + i, i, obtained);
        }
    }

}
