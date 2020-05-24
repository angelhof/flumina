package edu.upenn.flumina.pageview;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In Flink, the key-by operator uses a particular hash function to distribute keys across parallel
 * instances of a subsequent operator. Unfortunately, this hash function skews the distribution of keys,
 * and there's no way to replace it. Since our keys are user ids, to counteract the behavior of the
 * hash function, we generate the user ids by effectively invert the hash function, so that the hash
 * function maps the zeroth key to zero, first key to one, etc. This helper class does that.
 */
public class UserIdHelper {

    private static List<Integer> userIds;

    public static List<Integer> getUserIds(final int totalUsers) {
        if (userIds == null || userIds.size() != totalUsers) {
            final List<Integer> tempUserIds = new ArrayList<>(Collections.nCopies(totalUsers, -1));
            for (int i = 0, totalHit = 0; totalHit < totalUsers; ++i) {
                final int target = KeyGroupRangeAssignment.assignKeyToParallelOperator(i,
                        KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM, totalUsers);
                if (tempUserIds.get(target) == -1) {
                    tempUserIds.set(target, i);
                    ++totalHit;
                }
            }
            userIds = Collections.unmodifiableList(tempUserIds);
        }
        return userIds;
    }

}
