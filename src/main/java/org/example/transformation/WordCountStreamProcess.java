package org.example.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


public class WordCountStreamProcess {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndCount = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) {
                Arrays.stream(line.split("\\s+")).forEach(word -> {
                    collector.collect(Tuple2.of(word, 1L));
                });
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndCount.keyBy(tuple -> tuple.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);
        sum.print();
        env.execute();


    }
}
