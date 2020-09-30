package edu.upenn.flumina.pageview.data;

import edu.upenn.flumina.data.TimestampedUnion;

public interface GetOrUpdateOrHeartbeat extends TimestampedUnion<GetOrUpdate, GetOrUpdateHeartbeat> {

}
