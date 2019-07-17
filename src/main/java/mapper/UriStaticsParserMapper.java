package mapper;

import model.TimePeriodAndText;
import model.TimePeriodAndTwoInteger;
import model.TwoInteger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UriStaticsParserMapper extends Mapper<LongWritable, Text, TimePeriodAndTwoInteger, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\\s+");

        Integer partition = null;
        String uri = null;
        TimePeriodAndText timePeriodAndText = null;
        Integer a = null;
        Integer b = null;
        try {
            partition = Integer.valueOf(split[0]);
            uri = split[1];
            a = Integer.valueOf(split[2]);
            b = Integer.valueOf(split[3]);
        } catch (Exception e) {
            return;
        }
        if (timePeriodAndText != null && a != null && b != null)
            context.write(new TimePeriodAndTwoInteger(partition, new TwoInteger(a, b)), new Text(uri));
    }
}
