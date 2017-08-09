package mapreduce.jobs.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by liruibo on 2017/8/1.
 * 四个类型参数：
 *  1. mapper的 inputkey, inputvalue, outputkey, outputvalue
 */
public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {
        private final int N = 10;
        private TreeMap<Long, String> treeMap = new TreeMap<>();
        private LongWritable number = new LongWritable();

    /**
     * @param key 默认使用字节的偏移量作为key，LongWritable类型;若需使用其他类型，需在Job中进行设置。
     * @param line 默认使用一行作为值，类型为Text。
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
        public void map(LongWritable key, Text line, Context context)
            throws IOException,InterruptedException{

            treeMap.put(-Long.parseLong(line.toString()),"");
            if(treeMap.size()>N){
                treeMap.remove(treeMap.firstKey());//删除key最小元素
            }
}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        super.cleanup(context);
        for(Long k : treeMap.keySet()){
            context.write(NullWritable.get(),new LongWritable(-k));
        }

    }
}
