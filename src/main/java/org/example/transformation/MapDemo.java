package org.example.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> words = env.fromElements("hadoop", "spark", "flink", "hbase", "flink", "spark");

        SingleOutputStreamOperator<String> upperWords = words.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });
        upperWords.print();
        env.execute("MapDemo");
    }
}
