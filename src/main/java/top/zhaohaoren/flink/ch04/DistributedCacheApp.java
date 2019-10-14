package top.zhaohaoren.flink.ch04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

/**
 * @author zhaohaoren
 */
public class DistributedCacheApp {
    public static void main(String[] args) throws Exception {
        String path = "file:///Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter/src/main/java/top/zhaohaoren/flink/ch04/dependency/2";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 1. 注册分布式缓存
        env.registerCachedFile(path, "distributed_cache_alias");
        DataSource<String> data = env.fromElements("hadoop", "spark", "flink", "storm");

        data.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2. 获取分布式缓存文件
                File cacheFiles = getRuntimeContext().getDistributedCache().getFile("distributed_cache_alias");
                List<String> lines = FileUtils.readLines(cacheFiles);

                for (String line : lines) {
                    System.out.println(line);
                }
            }

            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).print();

    }

}
