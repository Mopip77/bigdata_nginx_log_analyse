package driver;

import comparator.DateAndStringComparator;
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
import org.apache.log4j.BasicConfigurator;
import partitioner.TimePeriodPartitioner;
import reducer.CountReducer;
import reducer.UriStaticsReducer;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
//        MyJob parserJob = new UriStaticsJob();
//        parserJob.setup(args[0], args[1], Driver.class);
//        System.exit(parserJob.run() ? 0 : 1);

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "asdf");
        job.setJarByClass(Driver.class);

        ChainMapper.addMapper(job, ParserLogMapper.class, LongWritable.class, Text.class, LogItem.class, IntWritable.class, new Configuration(false));
        ChainMapper.addMapper(job, UriExtractor.class, LogItem.class, IntWritable.class, DateAndString.class, LogItem.class, new Configuration(false));

        //TODO 有时间间隔则调用partitioner
        if (true) {
            job.setNumReduceTasks(3);
            job.setPartitionerClass(TimePeriodPartitioner.class);
        }

        job.setGroupingComparatorClass(DateAndStringComparator.class);
//        job.setSortComparatorClass(DateAndStringComparator.class);

//        job.setNumReduceTasks(1);
        ChainReducer.setReducer(job, UriStaticsReducer.class, DateAndString.class, LogItem.class, Text.class, Text.class, new Configuration(false));
//        ChainReducer.setReducer(job, CountReducer.class, DateAndString.class, LogItem.class, Text.class, IntWritable.class, new Configuration(false));
//        job.setReducerClass(UriStaticsReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
