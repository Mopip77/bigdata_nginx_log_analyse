package mapper;

import model.TimePeriodAndText;
import model.TwoInteger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IpStaticsParserMapper extends Mapper<LongWritable, Text, TwoInteger, TimePeriodAndText> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\\s+");

        Integer partition = null;
        String uri = null;
        Integer visitCount = null;
        Integer fluxCount = null;
        try {
            partition = Integer.valueOf(split[0]);
            uri = split[1];
            visitCount = Integer.valueOf(split[2]);
            fluxCount = Integer.valueOf(split[3]);
        } catch (Exception e) {
            return;
        }
        if (visitCount != null && fluxCount != null)
            context.write(new TwoInteger(visitCount, fluxCount), new TimePeriodAndText(partition, new Text(uri)));
    }
}
