package top.zhaohaoren.flink.ch05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 参考源码的demo
 * 自定义的一个source，通过实现SourceFunction方式。不支持并行
 * @author zhaohaoren
 */
public class CustomNonParallelSourceFunction implements SourceFunction<Long> {

    private Long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            // collect 发送数据
            sourceContext.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
