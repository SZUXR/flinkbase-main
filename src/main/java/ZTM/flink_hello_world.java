package ZTM;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

public class flink_hello_world {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStreamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        SingleOutputStreamOperator<Integer> mappedStream = dataStreamSource.map(x -> x * 2);

        mappedStream.print();
        env.execute();
    }
}


