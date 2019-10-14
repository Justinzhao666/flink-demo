package top.zhaohaoren.flink.ch03;

import org.apache.flink.api.common.functions.FlatMapFunction;
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

    public static class WC{
        private String word;
        private int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

         public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC(" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    ')';
        }
    }



    public static void main(String[] args) throws Exception {
        int port = 0;
        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("端口没有设置，使用默认端口9999");
            port = 9999;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("localhost", port);
        socket.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String s, Collector<WC> collector) throws Exception {
                String[] tokens = s.toLowerCase().split(" ");

                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new WC(token,1));
                    }
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print();
        env.execute("StreamingWCJavaApp");
    }

}
