package mapreduce.jobs.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by liruibo on 2017/8/9.
 */
public class TopNJob {
    public static void doTopNJob(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = new Job(conf, "[TopN RandomNum]");

        job.setJarByClass(TopNJob.class);//必须有
        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);//默认值为LongWritable.class
        job.setMapOutputValueClass(LongWritable.class);//默认值为Text.class

        job.setNumReduceTasks(1);

        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
