package workcount.streaming.custormSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义实现一个支持并行度的source
 *
 * RichParallelSourceFunction 会额外提供open和close的方法
 * 针对source中如果需要获取其他链接资源,那么可以在open方法中获取资源链接,在close中关闭资源
 */
public class MyRichParalleSource extends RichParallelSourceFunction<Long> {

    private long count = 1;

    private boolean isRuning  = true;
    /**
     * 主要的方法
     *
     * 启动一个source
     * 大部分情况下 都需要在这个润方法中实现一个循环  这样就可以循环产生数据了
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
     *取消一个cancel的时候会调用的方法
     */
    @Override
    public void cancel() {
        isRuning=false;
    }

    /**
     * 这个方法只会在最开始的时候调用
     * 实现获取链接的代码
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open............");
        super.open(parameters);
    }

    /**
     * 实现关闭连接的代码
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }
}
