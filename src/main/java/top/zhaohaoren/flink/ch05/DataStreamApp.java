package top.zhaohaoren.flink.ch05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        socketFunction(env);
        nonParallelSourceFunction(env);
        // stream 都要记得触发执行
        env.execute("DataStreamApp");
    }

    /**
     * 使用自定义的source
     */
    private static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        /*.setParallelism(2)    不支持并行的*/
//        DataStreamSource<Long> source = env.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> source = env.addSource(new CustomParallelSourceFunction()).setParallelism(2);

        source.print().setParallelism(1);
    }

    /**
     * socket source
     */
    private static void socketFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        source.print().setParallelism(1);
        //4> justin  4是代表并行度。print后面设置并行度为1就没这个问题了
    }
}
