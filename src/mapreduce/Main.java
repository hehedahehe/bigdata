package mapreduce;


import mapreduce.tools.ExistJobs;
import mapreduce.jobs.topn.TopNJob;

public class Main {

    public static void main(String[] args) throws Exception{
        if(args.length<3)
            throw new Exception("[useage] hadoop <jar_name>.jar <job_name> <input_path> <output_path>");
        String jobName = args[0].toLowerCase();
        String[] paths = new String[]{args[1],args[2]};
        switch (jobName){
            case ExistJobs.Sort_JOB:
                break;
            case ExistJobs.TOPN_JOB:
                TopNJob.doTopNJob(paths);
                break;
        }
    }





}

