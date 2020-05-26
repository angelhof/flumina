package edu.upenn.flumina.config;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * For now we're just reusing the value-barrier configuration
 */
public class FraudDetectionConfig extends ValueBarrierConfig {

    protected FraudDetectionConfig(final ParameterTool parameterTool) {
        super(parameterTool);
    }

}
