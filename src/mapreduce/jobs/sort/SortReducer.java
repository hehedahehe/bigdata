package mapreduce.jobs.sort;

import mapreduce.tools.MySort;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liruibo on 2017/8/9.
 */
public class SortReducer extends Reducer<NullWritable,LongWritable,NullWritable,LongWritable> {
    private List<Long> nums = new ArrayList<>();
    @Override
    protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for(LongWritable i : values){
            nums.add(i.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        new MySort<Long>().quickSort(nums);
        for(Long i : nums){
            context.write(NullWritable.get(),new LongWritable(i));
        }
    }
}
