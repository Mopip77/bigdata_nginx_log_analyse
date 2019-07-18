package mapper;

import model.TimePeriodAndText;
import model.TwoInteger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IpStaticsParserMapper extends Mapper<LongWritable, Text, IntWritable, TimePeriodAndText> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\\s+");

        Integer partition = null;
        String uri = null;
        Integer count = null;
        try {
            partition = Integer.valueOf(split[0]);
            uri = split[1];
            count = Integer.valueOf(split[2]);
        } catch (Exception e) {
            return;
        }
        if (count != null)
            context.write(new IntWritable(count), new TimePeriodAndText(partition, new Text(uri)));
    }
}
