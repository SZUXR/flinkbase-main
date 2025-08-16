package ZTM;

import bean.Order;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class test_late_events_processing {

    public static class OrderParser implements MapFunction<String, Order> {
        private final ObjectMapper mapper = new ObjectMapper();
        @Override
        public Order map(String jsonStr) throws Exception {
            JsonNode node = mapper.readTree(jsonStr);
            return new Order(
                    node.get("order_id").asText("unknown"),
                    node.get("customer_id").asText("unknown"),
                    node.get("product_id").asText("unknown"),
                    node.get("quantity").asInt(0),
                    node.get("price").asDouble(0.0),
                    node.get("order_time").asText("unknown")
            );
        }
    }

    public static class OrderTimestampAssigner implements SerializableTimestampAssigner<Order> {
        @Override
        public long extractTimestamp(Order order, long recordTimestamp) {
            return LocalDateTime.parse(order.orderTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        }
    }

    public static class AggregateWindowFunction extends ProcessWindowFunction<Order, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Order> elements, Collector<String> out) {
            int totalQuantity = 0;
            double totalSum = 0;

            for (Order order : elements) {
                totalQuantity += order.quantity;
                totalSum += order.quantity * order.price;
            }

            String windowStart = Instant.ofEpochMilli(context.window().getStart()).toString();
            String windowEnd = Instant.ofEpochMilli(context.window().getEnd()).toString();

            String result = String.format("{\"product_id\":\"%s\",\"total_quantity\":%d,\"total_spent\":%.2f,\"window_start\":\"%s\",\"window_end\":\"%s\"}",
                    key, totalQuantity, totalSum, windowStart, windowEnd);
            out.collect(result);
        }
    }



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Order> orderStream = socketStream.map(new OrderParser());
        orderStream.print("Parsed Orders");

        WatermarkStrategy<Order> watermarkStrategy = WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new OrderTimestampAssigner());

        OutputTag<Order> lateTag = new OutputTag<Order>("late-events") {
        };

        SingleOutputStreamOperator<String> windowedStream = orderStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(order -> order.productId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateTag)
                .process(new test_late_events_processing.AggregateWindowFunction());

        windowedStream.print("Aggregated");

        DataStream<Order> lateStream = windowedStream.getSideOutput(lateTag);
        lateStream.print("LateEvents");

        env.execute("Socket Late Events Processing");

    }
}
