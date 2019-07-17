package job;

import comparator.TwoIntegerReverseComparator;
import mapper.UriStaticsParserMapper;
import model.TimePeriodAndTwoInteger;
import model.TwoInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partitioner.TimePeriodPartitioner;
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
        job.setMapOutputKeyClass(TimePeriodAndTwoInteger.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(TimePeriodPartitioner.class);
        job.setSortComparatorClass(TwoIntegerReverseComparator.class);

        job.setNumReduceTasks(calNumTasks());
        job.setReducerClass(InverserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TwoInteger.class);

        FileInputFormat.setInputPaths(job, new Path(srcPath));
        FileOutputFormat.setOutputPath(job, new Path(destPath));
    }
}
