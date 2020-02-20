package workcount.streaming.streamingAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import workcount.streaming.custormSource.MyNoParalleSource;

/**
 * 但是只能连接两个流，两个流的数据类型可以不同，
 * 会对两个流中的数据应用不同的处理方法
 */
public class StreamingDemoConnect {
    public static void main(String[] args) throws Exception {

//获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        获取数据源
        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<String> text2_str = text2.map(new MapFunction<Long, String>() {

            @Override
            public String map(Long aLong) throws Exception {
                return "str_" + aLong;
            }
        });

        ConnectedStreams<Long, String> connectStream = text1.connect(text2_str);

        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long aLong) throws Exception {
                return aLong;
            }

            @Override
            public Object map2(String s) throws Exception {
                return s;
            }
        });
        result.print().setParallelism(1);

        String jobName = StreamExecutionEnvironment.class.getSimpleName();
        env.execute(jobName);


    }
}
