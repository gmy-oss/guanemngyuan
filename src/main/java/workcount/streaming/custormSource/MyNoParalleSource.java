package workcount.streaming.custormSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义实现并行度为1 的source
 *
 * 模拟产生从1开始的递增数字
 *
 *
 *
 *
 * 注意：
 *  SourceFunction  SourceContext 都要指定数据类型  如果不指定 就会报错
 *  Caused by: org.apache.flink.api.common.functions.InvalidTypesException:
 *  The types of the interface org.apache.flink.streaming.api.functions.source.SourceFunction could not be inferred.
 *  Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
 */
public class MyNoParalleSource implements SourceFunction<Long> {

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
