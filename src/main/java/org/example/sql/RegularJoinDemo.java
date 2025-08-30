package org.example.sql;

import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegularJoinDemo {
    public static void main(String[] args) {
        // 构造stream api 编程的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

        // 构造 sql 编程环境
        StreamTableEnvironment TableEnv = StreamTableEnvironment.create(env);

        // 1. 构造数据源映射表
        TableEnv.executeSql("CREATE TABLE kafka_table1 (" +
                "  uid BIGINT," +
                "  event_id STRING," +
                "  action_time BIGINT," +

                "  rt1 as to_timestamp_ltz(action_time,3)," +    // 事件时间列
                "  watermark for rt1 as rt1" +                    // 水位线定义
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'join1'," +
                "  'properties.bootstrap.servers' = '192.168.0.31:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")");

        TableEnv.executeSql("CREATE TABLE kafka_table2 (" +
                "  uid BIGINT," +
                "  face_status INT," +
                "  ts BIGINT," +

                "  rt2 as to_timestamp_ltz(ts,3)," +    // 事件时间列
                "  watermark for rt2 as rt2" +                    // 水位线定义
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'join2'," +
                "  'properties.bootstrap.servers' = '192.168.0.31:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")");
        
        // 2. 执行join查询
        TableEnv.executeSql("SELECT " +
                "  * " +
//                "FROM kafka_table1 JOIN kafka_table2 " +
                "FROM kafka_table1 LEFT JOIN kafka_table2 " +
                "ON kafka_table1.uid = kafka_table2.uid"
        ).print();
    }
}
