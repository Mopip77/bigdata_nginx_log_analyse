package job;

import comparator.DateAndSomeThingComaratorFactory;
import mapper.ParserLogMapper;
import mapper.UriExtractor;
import model.TimePeriodAndText;
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
import partitioner.TimePeriodPartitioner;
import reducer.UriStaticsReducer;

import java.io.IOException;

public class UriStaticsJob extends MyJob {

    public UriStaticsJob() {
        JOB_NAME = "uri_statics";
    }

    @Override
    public void setup(String srcPath, String destPath, Class driver) throws IOException {
        configuration = new Configuration();
        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        ChainMapper.addMapper(job, ParserLogMapper.class, LongWritable.class, Text.class, LogItem.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, UriExtractor.class, LogItem.class, IntWritable.class, TimePeriodAndText.class, LogItem.class, new Configuration(false));

        job.setNumReduceTasks(calNumTasks());
        job.setPartitionerClass(TimePeriodPartitioner.class);
        job.setGroupingComparatorClass(DateAndSomeThingComaratorFactory.getComparator(TimePeriodAndText.class, false, false));

        ChainReducer.setReducer(job, UriStaticsReducer.class, TimePeriodAndText.class, LogItem.class, TimePeriodAndText.class, Text.class, new Configuration(false));

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }
}

