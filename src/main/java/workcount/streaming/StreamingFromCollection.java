package workcount.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StreamingFromCollection {
    public static void main(String[] args) throws Exception {

//获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        指定数据源
        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(15);
        data.add(20);

        DataStreamSource<Integer> collectionData = env.fromCollection(data);

//        通map对数据进行处理
        DataStream<Integer> num =  collectionData.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {

                return value + 1;
            }
        });
//        直接打印

        num.print().setParallelism(1);
        env.execute("StreamingFromCollection");

    }
}
