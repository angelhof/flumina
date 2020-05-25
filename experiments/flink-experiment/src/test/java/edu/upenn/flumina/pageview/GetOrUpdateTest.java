package edu.upenn.flumina.pageview;

import edu.upenn.flumina.data.TimestampedUnion;
import edu.upenn.flumina.pageview.data.Get;
import edu.upenn.flumina.pageview.data.GetOrUpdate;
import edu.upenn.flumina.pageview.data.Heartbeat;
import edu.upenn.flumina.pageview.data.Update;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GetOrUpdateTest {

    @Test
    public void testGetOrUpdateMatch1() {
        final TimestampedUnion<GetOrUpdate, Heartbeat> get = new Get(1, 1);
        final int obtained = get.match(gou -> 1, hb -> 2);
        assertEquals("Match should return 1", 1, obtained);
    }

    @Test
    public void testGetOrUpdateMatch2() {
        final TimestampedUnion<GetOrUpdate, Heartbeat> update = new Update(1, 10000, 1);
        final int obtained = update.match(gou -> 1, hb -> 2);
        assertEquals("Match should return 1", 1, obtained);
    }

    @Test
    public void testGetOrUpdateMatch3() {
        final GetOrUpdate get = new Get(1, 1);
        final int obtained = get.match(g -> 1, u -> 2);
        assertEquals("Match should return 1", 1, obtained);
    }

    @Test
    public void testGetOrUpdateMatch4() {
        final GetOrUpdate update = new Update(1, 10000, 1);
        final int obtained = update.match(g -> 1, u -> 2);
        assertEquals("Match should return 2", 2, obtained);
    }

}
