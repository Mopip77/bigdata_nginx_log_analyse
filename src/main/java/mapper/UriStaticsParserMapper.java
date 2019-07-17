package mapper;

import model.TwoInteger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UriStaticsParserMapper extends Mapper<LongWritable, Text, TwoInteger, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\\s+");
        String uri = split[0];

        Integer a = null;
        Integer b = null;
        try {
            a = Integer.valueOf(split[1]);
            b = Integer.valueOf(split[2]);
        } catch (Exception e) {
            return;
        }
        if (a != null && b != null)
            context.write(new TwoInteger(a, b), new Text(uri));
    }
}
