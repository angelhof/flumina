package edu.upenn.flumina;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

public class ValueBarrierConfig {

    private static final int VALUE_NODES = 3;
    private static final int TOTAL_VALUES = 1_000_000;
    private static final double VALUE_RATE = 10.0;
    private static final int VALUE_BARRIER_RATIO = 1_000;
    private static final int HEARTBEAT_RATIO = 10;
    private static final String OUTPUT_FILE = Path.CUR_DIR + Path.SEPARATOR + "out.txt";
    private static final String STATISTICS_FILE = Path.CUR_DIR + Path.SEPARATOR + "statistics.txt";

    private final int valueNodes;
    private final int totalValues;
    private final double valueRate;
    private final int valueBarrierRatio;
    private final int heartbeatRatio;
    private final String outputFile;
    private final String statisticsFile;

    private ValueBarrierConfig(int valueNodes, int totalValues, double valueRate, int valueBarrierRatio,
                               int heartbeatRatio, String outputFile, String statisticsFile) {
        this.valueNodes = valueNodes;
        this.totalValues = totalValues;
        this.valueRate = valueRate;
        this.valueBarrierRatio = valueBarrierRatio;
        this.heartbeatRatio = heartbeatRatio;
        this.outputFile = outputFile;
        this.statisticsFile = statisticsFile;
    }

    public int getValueNodes() {
        return valueNodes;
    }

    public int getTotalValues() {
        return totalValues;
    }

    public double getValueRate() {
        return valueRate;
    }

    public int getValueBarrierRatio() {
        return valueBarrierRatio;
    }

    public int getHeartbeatRatio() {
        return heartbeatRatio;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public String getStatisticsFile() {
        return statisticsFile;
    }

    public static ValueBarrierConfig fromArgs(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        return new ValueBarrierConfig(
                parameterTool.getInt("valueNodes", VALUE_NODES),
                parameterTool.getInt("totalValues", TOTAL_VALUES),
                parameterTool.getDouble("valueRate", VALUE_RATE),
                parameterTool.getInt("vbRatio", VALUE_BARRIER_RATIO),
                parameterTool.getInt("hbRatio", HEARTBEAT_RATIO),
                parameterTool.get("outputFile", OUTPUT_FILE),
                parameterTool.get("statisticsFile", STATISTICS_FILE)
        );
    }
}
