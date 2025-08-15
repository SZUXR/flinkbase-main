package ZTM;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class windows_aggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .keyBy(x -> x % 2)
                .countWindow(2)
                .reduce((x, y) -> x + y)
                .print();

        env.execute();
    }
}
