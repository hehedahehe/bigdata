package mapreduce.jobs.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by liruibo on 2017/8/9.
 */
public class SortPartitioner extends Partitioner<NullWritable,IntWritable> {
    //总共有9*10^7个数字，数字的范围是1~100*10000
    private static final int Partion1 = 30*10000;
    private static final int Partion2 = 60*10000;

    @Override
    public int getPartition(NullWritable nullWritable, IntWritable intWritable, int numPartitions) {
        if(intWritable.get()<Partion1){
            return 0;
        }else if(intWritable.get()<=Partion2){
            return 1;
        }else {
            return 2;
        }
    }
}
