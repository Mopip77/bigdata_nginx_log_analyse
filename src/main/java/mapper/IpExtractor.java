package mapper;

import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import properties.MyProperties;

import java.io.IOException;
import java.io.PrintStream;

public class IpExtractor extends Mapper<LogItem, IntWritable, DateAndString, LogItem> {

    private static final int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    @Override
    protected void map(LogItem key, IntWritable value, Context context) throws IOException, InterruptedException {
        DateAndString extractKey = null;
        if (timePeriod == 0)
            extractKey = new DateAndString(0L, key.getIp());
        else
            extractKey = new DateAndString(key.getDate(), key.getIp());
        context.write(extractKey, key);
    }
}
