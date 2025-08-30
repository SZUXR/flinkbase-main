package org.example.sql;

import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CumulateWindowDemo {
    public static void main(String[] args) {
        // 构造stream api 编程的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

        // 构造 sql 编程环境
        StreamTableEnvironment TableEnv = StreamTableEnvironment.create(env);

        // 1. 构造数据源映射表
        TableEnv.executeSql("CREATE TABLE kafka_table (" +
                "  uid BIGINT," +
                "  event_id STRING," +
                "  properties MAP<STRING,STRING>," +
                "  action_time BIGINT," +

                "  rt as to_timestamp_ltz(action_time,3)," +    // 事件时间列
                "  watermark for rt as rt" +                    // 水位线定义
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'userlog1'," +
                "  'properties.bootstrap.servers' = '192.168.0.31:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")");
        
        // 执行查询sql：使用事件时间窗口
        TableEnv.executeSql("SELECT " +
                "  window_start, " +
                "  window_end, " +
                "  COUNT(DISTINCT uid) AS uv, " +
                "  COUNT(1) FILTER(WHERE event_id='page_load') AS pv " +
                "FROM TABLE(" +
                "  CUMULATE(TABLE kafka_table, DESCRIPTOR(rt), INTERVAL '1' MINUTE, INTERVAL '24' HOUR)" +
                ") " +
                "GROUP BY window_start, window_end"
        ).print();
    }
}
