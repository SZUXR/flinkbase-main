package dashboard;


import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时看板 指标计算任务
 * 今天每 5 分钟的pv数、uv数、会话数
 */
public class Job1_PVUV_1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建映射表，映射 kafka中的 dwd_events

        tableEnv.executeSql("CREATE TABLE dwd_kafka ( " +
                "  `user_id` BIGINT, " +
                "  `session_id` STRING, " +
                "  `event_id` STRING, " +
                "  `event_time` bigint, " +
                "  `proc_time` AS proctime(), " +
                "  `row_time` AS to_timestamp_ltz(event_time,3), " +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '0' SECOND " +
                ") WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = 'dwd_events', " +
                "  'properties.bootstrap.servers' = '192.168.0.31:9092', " +
                "  'properties.group.id' = 'testGroup', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'format' = 'json', " +
                "  'json.fail-on-missing-field' = 'false', " +
                "  'value.fields-include' = 'EXCEPT_KEY' " +
                ")");


        // 2. 创建映射表，映射StarRocks中的 流量看板表1
        tableEnv.executeSql(
                " CREATE TABLE dashboard_traffic_1 (            "
                        +"   window_start timestamp(3),            "
                        +"   window_end  timestamp(3),             "
                        +"   pv_amt   BIGINT,                      "
                        +"   uv_amt   BIGINT,                      "
                        +"   ses_amt   BIGINT                      "
                        +" ) WITH (                                          "
                        +"    'connector' = 'jdbc',                          "
                        +"    'url' = 'jdbc:mysql://192.168.0.33:9030/test1',     "
                        +"    'table-name' = 'dashboard_traffic_1',           "
                        +"    'username' = 'root',                           "
                        +"    'password' = '',                           "
                        +"    'driver' = 'com.mysql.cj.jdbc.Driver'          "
                        +" )                                                 "
        );




        // 3. 计算指标，并将结果输出到 目标存储
        // 2023-06-04 10:00:00,2023-06-04 10:05:00, 3259345,200203,178235
        // 逻辑： 开 5分钟 滚动窗口，在窗口内:
        // pv:sum(if事件=pageLoad,1,0) ,
        // uv: count(distinct user_id),
        // ses:count(distinct session_id)

        tableEnv.executeSql(
                "insert into dashboard_traffic_1 "
                        + " SELECT "
                        + " window_start, "
                        + " window_end, "
                        + " SUM(if(event_id='page_load',1,0)) as pv_amt, "
                        + " COUNT(distinct user_id) as uv_amt, "
                        + " COUNT(distinct session_id) as ses_amt "
                        + "FROM TABLE ( "
                        + "TUMBLE(TABLE dwd_kafka,DESCRIPTOR(row_time),INTERVAL '5' MINUTE)"
                        + ") "
                        + "GROUP BY window_start, window_end "
        );

    }
}
