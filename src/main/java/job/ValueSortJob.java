package job;

import comparator.SomeThingOfDateAndSomeThingComaratorFactory;
import comparator.TwoIntegerComparatorFactory;
import comparator.WritableInverseComparatorFactory;
import mapper.IpStaticsParserMapper;
import mapper.UriStaticsParserMapper;
import model.ThreeInteger;
import model.TimePeriodAndText;
import model.TwoInteger;
import myenum.StaticSortMainkey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partitioner.TimePeriodValuePartitioner;
import reducer.InverserReducer;

import java.io.IOException;

public class ValueSortJob extends MyJob {

    public ValueSortJob() {
        JOB_NAME = "value_sort_job";
    }

    public void setup(String srcPath, String destPath, Class driver) throws IOException {
        configuration = new Configuration();
        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        job.setMapperClass(UriStaticsParserMapper.class);
        job.setMapOutputKeyClass(ThreeInteger.class);
        job.setMapOutputValueClass(TimePeriodAndText.class);

        job.setPartitionerClass(TimePeriodValuePartitioner.class);
        // TODO
        configuration.setEnum("mainkey", StaticSortMainkey.FLUXCOUNT);
        job.setSortComparatorClass(ThreeInteger.getComparator(true, StaticSortMainkey.FLUXCOUNT.getKey()));

        job.setNumReduceTasks(calNumTasks());
        job.setReducerClass(InverserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ThreeInteger.class);

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }

    public void setup(String srcPath, String destPath, Class driver, Class sortComparator) throws IOException {
        configuration = new Configuration();
        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        job.setMapperClass(IpStaticsParserMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TimePeriodAndText.class);

        job.setPartitionerClass(TimePeriodValuePartitioner.class);
        // TODO
        job.setSortComparatorClass(WritableInverseComparatorFactory.getComparator(IntWritable.class));

        job.setNumReduceTasks(calNumTasks());
        job.setReducerClass(InverserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }
}
