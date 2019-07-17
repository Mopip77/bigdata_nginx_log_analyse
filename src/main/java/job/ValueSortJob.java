package job;

import comparator.StringOfDateAndStringComparator;
import comparator.TwoIntegerReverseComparator;
import mapper.ParserLogMapper;
import mapper.UriExtractor;
import mapper.UriStaticsParserMapper;
import model.DateAndString;
import model.LogItem;
import model.TwoInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partitioner.TimePeriodPartitioner;
import reducer.InverserReducer;
import reducer.UriStaticsReducer;

import java.io.IOException;

public class ValueSortJob implements MyJob {

    private Job job;
    private Configuration configuration;
    private static String JOB_NAME = "value_sort_job";

    public void setup(String srcPath, String destPath, Class driver) throws IOException {
        configuration = new Configuration();
        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        job.setMapperClass(UriStaticsParserMapper.class);
        job.setMapOutputKeyClass(TwoInteger.class);
        job.setMapOutputValueClass(Text.class);

        job.setSortComparatorClass(TwoIntegerReverseComparator.class);

//        job.setNumReduceTasks(0);
//        job.setOutputKeyClass(TwoInteger.class);
//        job.setOutputValueClass(Text.class);
        job.setReducerClass(InverserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TwoInteger.class);

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }

    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }

    public Job getJob() {
        return job;
    }
}
