package workcount.streaming.streamingAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import workcount.streaming.custormSource.MyNoParalleSource;

import java.util.ArrayList;

/**
 * 根据规则把一个数据流分为多个流
 *
 *应用场景
 *
 * 可能实际工作中，源数据中混合了多种类似的数据，多种类型的数据处理规则不一样，
 * 所以就可以在根据一定的规则
 *
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 *
 */
public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {

//获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource()).setParallelism(1);

//        对流进行切分，按照数据的奇偶性进行切分
        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long aLong) {
                ArrayList<String> outPut = new ArrayList<>();

                if (aLong % 2 == 0) {
                    outPut.add("even");
                } else {
                    outPut.add("odd");
                }
                return outPut;
            }
        });

//        选择一个或者多个切分后的流

        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> oddStream = splitStream.select("odd");

        DataStream<Long> moreStream = splitStream.select("odd","even");

        moreStream.print().setParallelism(1);

        String jobName = StreamExecutionEnvironment.class.getSimpleName();
        env.execute(jobName);


    }
}
