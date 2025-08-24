package org.example.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class MyTableFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> strDS = env.fromElements(
                "hello flink",
                "hello world hi",
                "hello java"
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table sensorTable = tableEnv.fromDataStream(strDS, $("words"));
        tableEnv.createTemporaryView("str", sensorTable);

        tableEnv.createFunction("SplitFunction", SplitFunction.class);

        tableEnv.sqlQuery("select words,newWord,newLength from str left join lateral table(SplitFunction(words))  as T(newWord,newLength) on true")
                .execute()
                .print();

    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String word: str.split(" ")) {
                collect(Row.of(word, word.length()));
            }
        }
    }
}
