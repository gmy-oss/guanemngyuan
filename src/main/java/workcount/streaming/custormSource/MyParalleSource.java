package workcount.streaming.custormSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义实现一个支持并行度的source
 *
 */
public class MyParalleSource implements ParallelSourceFunction<Long> {

    private long count = 1;

    private boolean isRuning  = true;
    /**
     * 主要的方法
     *
     * 启动一个source
     * 大部分情况下 都需要在这个润方法中实现一个循环  这样就可以循环产生数据了
     *
     *
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {

        while (isRuning){
            sourceContext.collect(count);
            count++;

//            每秒产生一条数据

            Thread.sleep(1000);

        }


    }

    /**
     *
     *
     *
     */
    @Override
    public void cancel() {
        isRuning=false;
    }
}
