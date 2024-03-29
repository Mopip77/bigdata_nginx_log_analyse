package job;

import comparator.DateAndSomeThingComaratorFactory;
import mapper.IpExtractor;
import mapper.ParserLogMapper;
import model.LogItem;
import model.TimePeriodAndText;
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
import partitioner.TimePeriodKeyPartitioner;
import reducer.IpStaticsReducer;

import java.io.IOException;

public class FluxStaticsJob extends MyJob {

    public FluxStaticsJob() {
        JOB_NAME = "flux_statics";
    }

    @Override
    public void setup(String srcPath, String destPath, Class driver) throws IOException {
        configuration = new Configuration();
        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        ChainMapper.addMapper(job, ParserLogMapper.class, LongWritable.class, Text.class, LogItem.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, IpExtractor.class, LogItem.class, IntWritable.class, TimePeriodAndText.class, LogItem.class, new Configuration(false));

        job.setNumReduceTasks(calNumTasks());
        job.setPartitionerClass(TimePeriodKeyPartitioner.class);
        job.setGroupingComparatorClass(DateAndSomeThingComaratorFactory.getComparator(TimePeriodAndText.class, false, false));

        ChainReducer.setReducer(job, IpStaticsReducer.class, TimePeriodAndText.class, LogItem.class, TimePeriodAndText.class, IntWritable.class, new Configuration(false));

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }
}

