package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink WordCount 示例程序
 */
public class App {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据源
        DataStream<String> text = env.fromElements(
            "Hello Flink",
            "Hello World",
            "Flink is great",
            "Apache Flink streaming"
        );

        // 处理数据：分词并计数
        DataStream<Tuple2<String, Integer>> wordCounts = text
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0)
            .sum(1);

        // 输出结果
        wordCounts.print();

        // 执行程序
        env.execute("Flink WordCount Example");
    }

    /**
     * 分词器
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 转换为小写并按空格分割
            String[] words = value.toLowerCase().split("\\W+");
            
            // 输出每个单词
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}