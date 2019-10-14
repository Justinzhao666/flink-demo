package top.zhaohaoren.flink.ch04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;
import java.util.List;

public class DataSetSinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        DataSource<Integer> source = env.fromCollection(data);
        String path = "/Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter/src/main/java/top/zhaohaoren/flink/ch04/dependency/";
        // 写到文件里
        source.writeAsText(path).setParallelism(2);
        // 记得最后触发下执行
        env.execute("DataSetSinkApp");
    }
}
