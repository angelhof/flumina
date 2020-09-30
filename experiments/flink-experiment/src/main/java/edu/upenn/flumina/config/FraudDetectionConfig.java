package edu.upenn.flumina.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

/**
 * For now we're just reusing the value-barrier configuration
 */
public class FraudDetectionConfig extends ValueBarrierConfig {

    private static final String TRANS_OUT_FILE = Path.CUR_DIR + Path.SEPARATOR + "trans.txt";

    private final String transOutFile;

    protected FraudDetectionConfig(final ParameterTool parameterTool) {
        super(parameterTool);
        this.transOutFile = parameterTool.get("transOutFile", TRANS_OUT_FILE);
    }

    public String getTransOutFile() {
        return transOutFile;
    }

}
