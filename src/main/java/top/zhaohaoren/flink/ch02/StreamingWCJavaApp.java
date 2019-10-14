package top.zhaohaoren.flink.ch02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用java来开发Flink的实时处理程序
 *
 * @author zhaohaoren
 */
public class StreamingWCJavaApp {
    public static void main(String[] args) throws Exception {

        // 我们可以将端口等值 通过参数传进来，flink也有相应的支持操作
        int port = 0;

        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口没有设置，使用默认端口9999");
            port = 9999;
        }

        // 1. 获取流的env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> socket = env.socketTextStream("localhost", port);
        // 3. 数据操作
        socket.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s /* 每次读取的一行的字符串 */, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split(" ");
                // 将每一个单词都组成一个tuple对象：(word,1)
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).keyBy(0) // 在流处理是keyBy
                .timeWindow(Time.seconds(5)) // 我们每隔多久执行一次这个操作逻辑
                .sum(1) // 对那一列进行求和。 对1那一列进行求和
                .print(); // 打印出来
        // 4. 流式操作，这个必须要有下面操作
        env.execute("StreamingWCJavaApp");
    }

    // 启动后需要 开启9999 端口，不然会报错Connection refused
    // 我们可以nc -lk 9999 来开启9999端口

}
