package edu.upenn.flumina.pageview;

import edu.upenn.flumina.pageview.data.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GetOrUpdateTest {

    @Test
    public void testGetOrUpdateOrHeartbeatMatch() {
        final GetOrUpdateOrHeartbeat get = new Get(1, 1);
        final GetOrUpdateOrHeartbeat update = new Update(1, 10000, 1);
        final GetOrUpdateOrHeartbeat heartbeat = new GetOrUpdateHeartbeat(1);

        final int getMatch = get.match(gou -> gou.match(g -> 1, u -> 2), hb -> 3);
        assertEquals("Get match should return 1", 1, getMatch);

        final int updateMatch = update.match(gou -> gou.match(g -> 1, u -> 2), hb -> 3);
        assertEquals("Update match should return 2", 2, updateMatch);

        final int heartbeatMatch = heartbeat.match(gou -> gou.match(g -> 1, u -> 2), hb -> 3);
        assertEquals("Heartbeat match should return 3", 3, heartbeatMatch);
    }

    @Test
    public void testGetOrUpdateMatch() {
        final GetOrUpdate get = new Get(1, 1);
        final GetOrUpdate update = new Update(1, 10000, 1);

        final int getMatch = get.match(g -> 1, u -> 2);
        assertEquals("Get match should return 1", 1, getMatch);

        final int updateMatch = update.match(g -> 1, u -> 2);
        assertEquals("Match should return 2", 2, updateMatch);
    }

}
