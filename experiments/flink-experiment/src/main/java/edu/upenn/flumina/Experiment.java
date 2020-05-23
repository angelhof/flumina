package edu.upenn.flumina;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Experiment {

    JobExecutionResult run(StreamExecutionEnvironment env, long startTime) throws Exception;

    long getTotalEvents();

    long getOptimalThroughput();

}
