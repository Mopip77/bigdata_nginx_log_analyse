package mapper;

import job.MyJob;
import model.TimePeriodAndText;
import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import properties.MyProperties;

import java.io.IOException;

public class IpExtractor extends Mapper<LogItem, IntWritable, TimePeriodAndText, LogItem> {

//    private static final long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static final int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    @Override
    protected void map(LogItem key, IntWritable value, Context context) throws IOException, InterruptedException {
        TimePeriodAndText extractKey = null;
        if (timePeriod == 0)
            extractKey = new TimePeriodAndText(0, new Text(key.getIp()));
        else {
            int periodNum = MyJob.calNumTasks();
            extractKey = new TimePeriodAndText(periodNum, new Text(key.getIp()));
        }
        context.write(extractKey, key);
    }
}
