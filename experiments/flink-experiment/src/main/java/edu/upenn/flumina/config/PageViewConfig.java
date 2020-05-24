package edu.upenn.flumina.config;

import org.apache.flink.api.java.utils.ParameterTool;

public class PageViewConfig extends Config {

    // Default parameter values specific to the PageView experiment
    private static final int TOTAL_PAGEVIEWS = 500_000;
    private static final int TOTAL_USERS = 2;
    private static final int PAGEVIEW_PARALLELISM = 1;
    private static final double PAGEVIEW_RATE = 1.0;

    private final int totalPageViews;
    private final int totalUsers;
    private final int pageViewParallelism;
    private final double pageViewRate;

    protected PageViewConfig(final ParameterTool parameterTool) {
        super(parameterTool);
        this.totalPageViews = parameterTool.getInt("totalPageViews", TOTAL_PAGEVIEWS);
        this.totalUsers = parameterTool.getInt("totalUsers", TOTAL_USERS);
        this.pageViewParallelism = parameterTool.getInt("pageViewParallelism", PAGEVIEW_PARALLELISM);
        this.pageViewRate = parameterTool.getDouble("pageViewRate", PAGEVIEW_RATE);
    }

    public int getTotalPageViews() {
        return totalPageViews;
    }

    public int getTotalUsers() {
        return totalUsers;
    }

    public int getPageViewParallelism() {
        return pageViewParallelism;
    }

    public double getPageViewRate() {
        return pageViewRate;
    }

}
