package edu.upenn.flumina.pageview;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageViewGeneratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(PageViewGeneratorTest.class);

    @Test
    public void testPageViewGenerator() {
        final var pageViewGenerator = new PageViewGenerator(10, 2, 10.0);
        pageViewGenerator.getIterator()
                .forEachRemaining(item -> LOG.debug(item.toString()));
    }

}
