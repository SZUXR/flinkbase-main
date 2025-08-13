package org.example.window;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowOperatorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] values = line.split("\\s+");
                    return Tuple2.of(values[0], Long.parseLong(values[1]));
                }, Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .countWindow(10, 3)
                .sum(1)
                .print();
        env.execute("windowing");
    }
}
