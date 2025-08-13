package org.example.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TcpSocketSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        env.socketTextStream("localhost", 4567, "\n")
                .setParallelism(1)
                .print();
        env.execute("TcpSourceSourceCollectorExample");
    }
}
