package job;

import mapper.ParserLogMapper;
import mapper.UriExtractor;
import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.UriStaticsReducer;

import java.io.IOException;

public class UriStaticsJob implements MyJob {

    private Job job;
    private Configuration configuration;
    private static String JOB_NAME = "uri_statics_analyze";

    public void setup(String srcPath, String OutputPath, Class driver) throws IOException {
        configuration = new Configuration();
//        configuration.setQuietMode(false);
        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        ChainMapper.addMapper(job, ParserLogMapper.class, LongWritable.class, Text.class, LogItem.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, UriExtractor.class, LogItem.class, IntWritable.class, DateAndString.class, LogItem.class, new Configuration(false));
//        job.setMapOutputKeyClass(DateAndBasetype.class);
//        job.setMapOutputValueClass(LogItem.class);

        //TODO 有时间间隔则调用partitioner
//        if (true)
//            job.setPartitionerClass(TimePeriodPartitioner.class);

//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setGroupingComparatorClass(DateAndStringComparator.class);
//        job.setSortComparatorClass(DateAndStringComparator.class);

        ChainReducer.setReducer(job, UriStaticsReducer.class, DateAndString.class, LogItem.class, Text.class, Text.class, new Configuration(false));
//        job.setReducerClass(UriStaticsReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);

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

