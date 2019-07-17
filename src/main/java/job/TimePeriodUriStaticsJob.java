package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partitioner.TimePeriodPartitioner;

import java.io.IOException;

public class TimePeriodUriStaticsJob implements MyJob {

    private Job job;
    private Configuration configuration;
    private static String JOB_NAME = "uri_statics";

    public void setup(String srcPath, String OutputPath, Class driver) throws IOException {
        configuration = new Configuration();
        job = Job.getInstance(configuration, JOB_NAME);

//        job.setMapperClass(TimePeriodStaticsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(TimePeriodPartitioner.class);
//        job.setSortComparatorClass();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));
    }

    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }

    public Job getJob() {
        return job;
    }
}
