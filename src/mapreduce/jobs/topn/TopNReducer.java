package mapreduce.jobs.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by liruibo on 2017/8/1.
 */
public class TopNReducer extends Reducer<NullWritable,LongWritable,NullWritable,LongWritable> {
    private int N = 10;
    private TreeMap<Long,String>  topNRecords= new TreeMap();
    private TreeMap<Long,String>  topNRecordsBak= new TreeMap();
    private LongWritable number = new LongWritable();
    public void reduce(NullWritable key,Iterable<LongWritable> values,Context context)
            throws IOException,InterruptedException{

        for(LongWritable rec : values){
            topNRecords.put(-rec.get(),"");
            if(topNRecords.size()>N){
                topNRecords.remove(topNRecords.firstKey());//删除key值最小元素，删除-500而不是-1
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        super.cleanup(context);
        for(Long l : topNRecords.keySet()){
            topNRecordsBak.put(-l,"");

        }
        for(Long l : topNRecordsBak.keySet()){
            number.set(l);
            context.write(NullWritable.get(),number);
        }

    }
}
