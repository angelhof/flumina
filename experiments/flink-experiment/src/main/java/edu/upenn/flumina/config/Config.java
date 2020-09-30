package edu.upenn.flumina.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

public class Config {

    // Default parameter values
    private static final long INIT_SYNC_DELAY = 2_000L;
    private static final String OUT_FILE = Path.CUR_DIR + Path.SEPARATOR + "out.txt";
    private static final String STATS_FILE = Path.CUR_DIR + Path.SEPARATOR + "stats.txt";

    private final String experiment;
    private final boolean manual;
    private final String rmiHost;
    private final long initSyncDelay;
    private final String outFile;
    private final String statsFile;

    protected Config(final ParameterTool parameterTool) {
        this.experiment = parameterTool.get("experiment");
        this.manual = parameterTool.getBoolean("manual", false);
        this.rmiHost = parameterTool.get("rmiHost", "localhost");
        this.initSyncDelay = parameterTool.getLong("initSyncDelay", INIT_SYNC_DELAY);
        this.outFile = parameterTool.get("outFile", OUT_FILE);
        this.statsFile = parameterTool.get("statsFile", STATS_FILE);
    }

    public String getExperiment() {
        return experiment;
    }

    public boolean isManual() {
        return manual;
    }

    public String getRmiHost() {
        return rmiHost;
    }

    /**
     * Delay in milliseconds for synchronizing the sources
     *
     * @return Delay in milliseconds
     */
    public long getInitSyncDelay() {
        return initSyncDelay;
    }

    public String getOutFile() {
        return outFile;
    }

    public String getStatsFile() {
        return statsFile;
    }

    public static Config fromArgs(final String[] args) throws ConfigException {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String experiment = parameterTool.get("experiment", "");
        switch (experiment) {
            case "value-barrier":
            case "value-barrier-seq":
                return new ValueBarrierConfig(parameterTool);
            case "pageview":
            case "pageview-seq":
                return new PageViewConfig(parameterTool);
            case "fraud-detection":
                return new FraudDetectionConfig(parameterTool);
            default:
                throw new ConfigException("Experiment was either not specified or it does not exist.");
        }
    }

}
