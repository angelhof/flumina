package edu.upenn.flumina.util;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.*;

/**
 * In Flink, the key-by operator uses a particular hash function to distribute keys across parallel
 * instances of a subsequent operator. Unfortunately, this hash function skews the distribution of keys,
 * and there's no way to replace it. This helper class inverts the hash function, so that the hash
 * function maps the zeroth key to zero, first key to one, etc.
 */
public class FlinkHashInverter {

    private static final Map<Integer, List<Integer>> cachedMappings = new HashMap<>();

    /**
     * Precondition: {@code 0 <= key < totalKeys}
     *
     * @param key       Key we want to be inverted
     * @param totalKeys Total number of keys
     * @return A number that Flink's hash function will map to {@code key}
     */
    public static int invert(final int key, final int totalKeys) {
        return getMapping(totalKeys).get(key);
    }

    public static List<Integer> getMapping(final int totalKeys) {
        if (!cachedMappings.containsKey(totalKeys)) {
            final List<Integer> mapping = new ArrayList<>(Collections.nCopies(totalKeys, -1));
            for (int i = 0, totalHit = 0; totalHit < totalKeys; ++i) {
                final int target = KeyGroupRangeAssignment.assignKeyToParallelOperator(i,
                        KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM, totalKeys);
                if (mapping.get(target) == -1) {
                    mapping.set(target, i);
                    ++totalHit;
                }
            }
            cachedMappings.put(totalKeys, Collections.unmodifiableList(mapping));
        }
        return cachedMappings.get(totalKeys);
    }

}
