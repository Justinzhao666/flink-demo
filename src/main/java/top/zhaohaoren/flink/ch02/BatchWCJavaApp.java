package top.zhaohaoren.flink.ch02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用java api 来开发的flink批处理应用
 *
 * @author zhaohaoren
 */
public class BatchWCJavaApp {

    public static void main(String[] args) throws Exception {
        String path = "file:///Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter/src/main/java/top/zhaohaoren/flink/ch02/input";

        // 1. 设置上下文环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataSource<String> textFile = env.readTextFile(path);
        textFile.print(); // 打印一下
        // 3. 数据处理
        textFile
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
                })
                .groupBy(0) // 分组，0 的意思按照tuple中第几个位置进行分组，0就是代表按照第一列分组，即word
                .sum(1) // 对那一列进行求和。 对1那一列进行求和
                .print(); // 打印出来
    }
}
