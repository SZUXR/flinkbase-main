package ZTM;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class orders_processing {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(
                        new Order("order-1", 100, "user-1"),
                        new Order("order-2", 200, "user-2"),
                        new Order("order-3", 300, "user-1"),
                        new Order("order-4", 400, "user-1"),
                        new Order("order-5", 500, "user-2")
                ).keyBy(order -> order.userId)
                .process(new OrderProcessFunction())
                .print();

        env.execute("Order Processing Job");
    }

    public static class Order {
        public String orderId;
        public double amount;
        public String userId;

        public Order(String orderId, double amount, String userId) {
            this.orderId = orderId;
            this.amount = amount;
            this.userId = userId;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "orderId='" + orderId + '\'' +
                    ", amount=" + amount +
                    ", userId='" + userId + '\'' +
                    '}';
        }
    }

    public static class OrderProcessFunction extends KeyedProcessFunction<String, Order, String> {
        private double totalAmount = 0;
        private int orderCount = 0;

        @Override
        public void processElement(Order order, Context context, Collector<String> out) {
            totalAmount += order.amount;
            orderCount++;

            String result = String.format("User: %s, Order: %s, Amount: %.2f, Total: %.2f, Count: %d",
                    order.userId, order.orderId, order.amount, totalAmount, orderCount);
            out.collect(result);
        }
    }
}