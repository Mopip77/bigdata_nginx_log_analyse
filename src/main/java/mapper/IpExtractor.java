package mapper;

import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IpExtractor extends Mapper<LogItem, IntWritable, DateAndString, LogItem> {
    @Override
    protected void map(LogItem key, IntWritable value, Context context) throws IOException, InterruptedException {
        DateAndString extractKey = new DateAndString(key.getDate(), key.getIp());
        context.write(extractKey, key);
    }
}
