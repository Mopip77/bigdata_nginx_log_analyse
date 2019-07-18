package mapper;

import model.LogItem;
import model.TimePeriodAndText;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import properties.MyProperties;

import java.io.IOException;

public class FluxExtractor extends Mapper<LogItem, IntWritable, TimePeriodAndText, LogItem> {

    private static final long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static final int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    @Override
    protected void map(LogItem key, IntWritable value, Context context) throws IOException, InterruptedException {
        TimePeriodAndText extractKey = null;
        if (timePeriod == 0)
            extractKey = new TimePeriodAndText(0, new Text(key.getIp()));
        else {
            Long date = key.getDate();
            int periodNum = (int) ((date - startTime) / timePeriod);
            extractKey = new TimePeriodAndText(periodNum, new Text(key.getIp()));
        }
        context.write(extractKey, key);
    }
}
