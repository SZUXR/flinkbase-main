package org.example.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = env.fromElements("hbase", "hadoop", "flink");

        SingleOutputStreamOperator<String> filtered = words.filter(w -> w.startsWith("h"));


        filtered.print();
        env.execute("FilterDemo");
    }
}
