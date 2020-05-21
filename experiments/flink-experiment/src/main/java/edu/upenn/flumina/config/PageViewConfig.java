package edu.upenn.flumina.config;

import org.apache.flink.api.java.utils.ParameterTool;

public class PageViewConfig extends Config {

    // Default parameter values specific to the PageView experiment

    protected PageViewConfig(final ParameterTool parameterTool) {
        super(parameterTool);
    }

}
