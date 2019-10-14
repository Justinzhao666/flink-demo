package top.zhaohaoren.flink.ch05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义的一个source，通过实现ParallelSourceFunction方式。支持并行
 * @author zhaohaoren
 */
public class CustomParallelSourceFunction implements ParallelSourceFunction<Long> {

    private Long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
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
