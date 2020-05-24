package edu.upenn.flumina.pageview;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class UserIdHelperTest {

    private static final Logger LOG = LoggerFactory.getLogger(UserIdHelperTest.class);

    @Test
    public void testGetUserIds() {
        final int totalUsers = 10;
        final List<Integer> userIds = UserIdHelper.getUserIds(totalUsers);
        assertEquals("userIds should have correct size", totalUsers, userIds.size());
        for (int i = 0; i < totalUsers; ++i) {
            final int obtained = KeyGroupRangeAssignment.assignKeyToParallelOperator(userIds.get(i),
                    KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM, totalUsers);
            assertEquals("userId at position " + i + " should map to " + i, i, obtained);
        }
    }

}
