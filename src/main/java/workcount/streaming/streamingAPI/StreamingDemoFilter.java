package workcount.streaming.streamingAPI;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import workcount.streaming.custormSource.MyNoParalleSource;

/**
 * 过滤函数，对传入的数据进行判断，符合条件的数据会被留下
 */
public class StreamingDemoFilter {
    public static void main(String[] args) throws Exception {

//获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStream<Long> num = text.map(new MapFunction<Long,Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据："+value);
                return value;
            }
        });

//        执行filter过滤，满足条件的数据会被留下
        DataStream<Long> filterData =  num.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {

                return value % 2 ==0;
            }
        });

        DataStream<Long> resultData =  filterData.map(new MapFunction<Long,Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后的数据："+value);
                return value;
            }
        });


//        每2秒钟处理一次数据
        SingleOutputStreamOperator<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        String jobName = StreamExecutionEnvironment.class.getSimpleName();
        env.execute(jobName);


    }
}
