package mapreduce.jobs.topn;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by liruibo on 2017/8/5.
 */
public class TopNPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE>{
    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        return 0;
    }
}
