package top.zhaohaoren.flink.ch05;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义的一个source，通过继承RichParallelSourceFunction方式
 * @author zhaohaoren
 */
public class CustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {

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
