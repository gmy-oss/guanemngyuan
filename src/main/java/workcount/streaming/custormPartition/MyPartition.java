package workcount.streaming.custormPartition;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int i) {
        System.out.println("分区总数："+i);
        if (key % 2 == 0){
            return 0;
        }else{
            return 1;
        }
    }
}
