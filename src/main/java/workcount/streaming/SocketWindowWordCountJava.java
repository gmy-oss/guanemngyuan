package workcount.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 *
 * 每隔一秒统计最近2秒内的数据 打印到控制台
 *
 *
 */
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
//        获取需要的端口号
        int port;
        try{

            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("No post set . use default port 9000---java");
            port = 9000;
        }

        // 1、获取flink的运行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "hadoop102";
        String delimiter = "\n";
//       /2、链接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

//        3、进行算子计算
        DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                String[] split = value.split("\\s");
                for (String word:split) {
                    collector.collect(new WordWithCount(word,1L));
                }
            }
//            keyBy对单词进行分组
        }).keyBy("word").timeWindow(Time.seconds(2),Time.seconds(1))//指定时间窗口大小为两秒 间隔时间为1秒
          .sum("count");//在这里使用sum 或者reduce都可以

//        .reduce(new ReduceFunction<WordWithCount>() {
//            public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
//                return new WordWithCount(a.word,a.count+b.count);
//            }
//        });

//        把数据打印到控制台 并且设置并行度
        windowCount.print().setParallelism(1);

//        ！！！ 这一行代码一定要实现 否则程序不实行
        env.execute("Socket window count");
    }


    public static class WordWithCount{
        public String word;
        public long count;
        public  WordWithCount(){}
       public WordWithCount(String word,long count){
           this.word = word;
           this.count = count;
       }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
