package job;

import comparator.TwoIntegerComparatorFactory;
import mapper.IpStaticsParserMapper;
import mapper.UriStaticsParserMapper;
import model.ThreeInteger;
import model.TimePeriodAndText;
import model.TwoInteger;
import myenum.SortMainkey;
import myenum.VFSortMainkey;
import myenum.VIFSortMainkey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partitioner.TimePeriodValuePartitioner;
import reducer.InverserReducer;
import reducer.IpDetailStaticsReducer;

import java.io.IOException;

public class ValueSortJob extends MyJob {

    public ValueSortJob() {
        JOB_NAME = "value_sort_job";
    }

    public void setup(String srcPath, String destPath, Class driver) throws IOException {
        configuration = new Configuration();
        SortMainkey mainkey = VIFSortMainkey.FLUXCOUNT;
        configuration.set("mainkey", mainkey.getName());

        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        job.setMapperClass(UriStaticsParserMapper.class);
        job.setMapOutputKeyClass(ThreeInteger.class);
        job.setMapOutputValueClass(TimePeriodAndText.class);

        job.setPartitionerClass(TimePeriodValuePartitioner.class);
        // TODO
        job.setSortComparatorClass(ThreeInteger.getComparator(true, mainkey.getKey()));

        job.setNumReduceTasks(calNumTasks());
        job.setReducerClass(InverserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ThreeInteger.class);

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }

    public void setup(String srcPath, String destPath, Class driver, Class sortComparator) throws IOException {
        configuration = new Configuration();
        SortMainkey mainkey = VFSortMainkey.FLUXCOUNT;
        configuration.set("mainkey", mainkey.getName());
        job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(driver);

        job.setMapperClass(IpStaticsParserMapper.class);
        job.setMapOutputKeyClass(TwoInteger.class);
        job.setMapOutputValueClass(TimePeriodAndText.class);

        job.setPartitionerClass(TimePeriodValuePartitioner.class);
        // TODO
        System.out.println(configuration.hashCode());
        job.setSortComparatorClass(TwoInteger.getComparator(true, mainkey.getKey()));

        job.setNumReduceTasks(calNumTasks());
        job.setReducerClass(InverserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }
}
