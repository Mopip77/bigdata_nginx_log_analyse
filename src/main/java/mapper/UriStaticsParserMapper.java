package mapper;

import model.ThreeInteger;
import model.TimePeriodAndText;
import model.TimePeriodAndTwoInteger;
import model.TwoInteger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UriStaticsParserMapper extends Mapper<LongWritable, Text, ThreeInteger, TimePeriodAndText> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\\s+");

        Integer partition = null;
        String uri = null;
        Integer a = null;
        Integer b = null;
        Integer c = null;
        try {
            partition = Integer.valueOf(split[0]);
            uri = split[1];
            a = Integer.valueOf(split[2]);
            b = Integer.valueOf(split[3]);
            c = Integer.valueOf(split[4]);
        } catch (Exception e) {
            return;
        }
        if (a != null && b != null && c != null)
            context.write(new ThreeInteger(a, b, c), new TimePeriodAndText(partition, new Text(uri)));
    }
}
