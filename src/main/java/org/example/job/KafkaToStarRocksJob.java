package org.example.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.UserBehavior;

import java.text.SimpleDateFormat;
import java.util.Date;

public class KafkaToStarRocksJob {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 1. 配置Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.0.31:9092")
                .setTopics("user_behavior")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // 2. 从Kafka读取数据
        DataStream<String> kafkaStream = env.fromSource(source, 
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), 
                "Kafka Source");
        
        // 3. 数据转换和处理
        DataStream<UserBehavior> processedStream = kafkaStream
                .map(new JsonToUserBehaviorMapper())
                .map(new UserBehaviorProcessor());
        
        // 4. 配置JDBC Sink到StarRocks
        String insertSQL = "INSERT INTO user_behavior_result (userId, itemId, timestamp, categoryId, behavior, date, hour, behaviorCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://192.168.0.33:9030/test1")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("")
                .build();
        
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();
        
        // 5. 写入StarRocks
        processedStream.addSink(JdbcSink.sink(
                insertSQL,
                (statement, userBehavior) -> {
                    statement.setLong(1, userBehavior.getUserId());
                    statement.setString(2, userBehavior.getItemId());
                    statement.setLong(3, userBehavior.getTimestamp());
                    statement.setString(4, userBehavior.getCategoryId());
                    statement.setString(5, userBehavior.getBehavior());
                    statement.setString(6, userBehavior.getDate());
                    statement.setInt(7, userBehavior.getHour());
                    statement.setLong(8, userBehavior.getBehaviorCount());
                },
                executionOptions,
                connectionOptions
        ));
        
        // 6. 执行任务
        env.execute("Kafka to StarRocks Job");
    }
    
    // JSON字符串转UserBehavior对象
    public static class JsonToUserBehaviorMapper implements MapFunction<String, UserBehavior> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        
        @Override
        public UserBehavior map(String jsonString) throws Exception {
            return objectMapper.readValue(jsonString, UserBehavior.class);
        }
    }
    
    // 数据处理逻辑
    public static class UserBehaviorProcessor implements MapFunction<UserBehavior, UserBehavior> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        
        @Override
        public UserBehavior map(UserBehavior userBehavior) throws Exception {
            // 处理时间戳，提取日期和小时
            Date date = new Date(userBehavior.getTimestamp() * 1000);
            userBehavior.setDate(dateFormat.format(date));
            userBehavior.setHour(date.getHours());
            
            // 设置行为计数（这里简单设为1，实际可以根据业务逻辑处理）
            userBehavior.setBehaviorCount(1L);
            
            return userBehavior;
        }
    }
}