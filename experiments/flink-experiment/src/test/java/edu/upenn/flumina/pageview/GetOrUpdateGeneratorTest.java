package edu.upenn.flumina.pageview;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetOrUpdateGeneratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(GetOrUpdateGeneratorTest.class);

    @Test
    public void testGetOrUpdateGenerator() {
        final var getOrUpdateGenerator = new GetOrUpdateGenerator(1000, 2, 10.0);
        getOrUpdateGenerator.getIterator()
                .forEachRemaining(item -> LOG.debug(item.toString()));
    }

}
