package top.zhaohaoren.flink.ch04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/batch/#data-sources
 */
public class DataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);
//        fromFile(env);
//        fromCsv(env);
        fromCompress(env);
    }

    // data source 从集合的方式
    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }

    // data source 从文件的方式
    private static void fromFile(ExecutionEnvironment env) throws Exception {
        // flink 可以指定一个文件去读，也可以指定一个文件夹去读。
        String path = "file:///Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter-scala/src/main/scala/top/zhaohaoren/flink/scala/ch02/input";
        env.readTextFile(path).print();
    }

    // data source 从csv文件方式
    private static void fromCsv(ExecutionEnvironment env) throws Exception {
        String path = "file:///Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter/src/main/java/top/zhaohaoren/flink/ch04/people.csv";

        env.readCsvFile(path).ignoreFirstLine().includeFields("110").types(String.class, Integer.class).print();
    }

    // data source 递归文件夹下读取
    private static void fromDir(ExecutionEnvironment env) throws Exception {
        String path = "file:///Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter-scala/src/main/scala/top/zhaohaoren/flink/scala/ch04/data";
        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(path).withParameters(configuration).print();
    }

    // data source 从压缩文件
    private static void fromCompress(ExecutionEnvironment env) throws Exception {
        String path = "file:///Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter-scala/src/main/scala/top/zhaohaoren/flink/scala/ch04/data/a.tar.gz";
        env.readTextFile(path).print(); // 直接就可以用
    }
}
