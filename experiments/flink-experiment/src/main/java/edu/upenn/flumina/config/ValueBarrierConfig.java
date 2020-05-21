package edu.upenn.flumina.config;

import org.apache.flink.api.java.utils.ParameterTool;

public class ValueBarrierConfig extends Config {

    // Default parameter values specific to the ValueBarrier experiment
    private static final int VALUE_NODES = 3;
    private static final int TOTAL_VALUES = 1_000_000;
    private static final double VALUE_RATE = 10.0;
    private static final int VALUE_BARRIER_RATIO = 1_000;
    private static final int HEARTBEAT_RATIO = 10;

    private final int valueNodes;
    private final int totalValues;
    private final double valueRate;
    private final int valueBarrierRatio;
    private final int heartbeatRatio;

    protected ValueBarrierConfig(final ParameterTool parameterTool) {
        super(parameterTool);
        this.valueNodes = parameterTool.getInt("valueNodes", VALUE_NODES);
        this.totalValues = parameterTool.getInt("totalValues", TOTAL_VALUES);
        this.valueRate = parameterTool.getDouble("valueRate", VALUE_RATE);
        this.valueBarrierRatio = parameterTool.getInt("vbRatio", VALUE_BARRIER_RATIO);
        this.heartbeatRatio = parameterTool.getInt("hbRatio", HEARTBEAT_RATIO);
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

}
