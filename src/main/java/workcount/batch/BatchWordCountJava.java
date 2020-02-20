package workcount.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {


        String inputpath = "D:\\data\\file";
        String outputpath = "D:\\data\\result";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile(inputpath);
       DataSet<Tuple2<String,Integer>>counts = text.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);

        counts.writeAsCsv(outputpath,"\n"," ").setParallelism(1);
        env.execute("batch word count");
    }

    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{

        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = value.toLowerCase().split("\\w+");
            for (String token: tokens
                 ) {
                if (token.length()>0){
                    collector.collect(new Tuple2<String, Integer>(token,1));
                }
            }
        }
    }
}
