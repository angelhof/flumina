package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.TimestampedUnion;

public interface PageViewOrHeartbeat extends TimestampedUnion<PageView, PageViewHeartbeat> {

    int getUserId();

}
