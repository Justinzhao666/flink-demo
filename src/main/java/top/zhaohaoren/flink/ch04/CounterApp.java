package top.zhaohaoren.flink.ch04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * 获得java计数器
 *
 * @author zhaohaoren
 */
public class CounterApp {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("hadoop", "spark", "flink", "storm");
        DataSet<String> source = data.map(new RichMapFunction<String, String>() {
            // 1. 先定义计数器
            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2. 注册计数器- 在runtime中加入一个计数器，对该计数器可以起一个名字
                getRuntimeContext().addAccumulator("ele-count-java", counter);
            }

            @Override
            public String map(String s) throws Exception {
                // 3. 累加counter计数器
                counter.add(1L);
                return s;
            }
        }).setParallelism(4);
        // 4. 要求必须有个sink
        String path = "file:///Users/zhaohaoren/workspace/code/mine/JavaProjects/flink-starter/src/main/java/top/zhaohaoren/flink/ch04/dependency/counter.txt";
        source.writeAsText(path, FileSystem.WriteMode.OVERWRITE);
        // 5. 获取计数器
        JobExecutionResult result = env.execute("CounterApp");
        long count = result.getAccumulatorResult("ele-count-java");
        System.out.println(count);
    }
}
