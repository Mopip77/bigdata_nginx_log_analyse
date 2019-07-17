package mapper;

import model.TimePeriodAndText;
import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import properties.MyProperties;

import java.io.IOException;

public class UriExtractor extends Mapper<LogItem, IntWritable, TimePeriodAndText, LogItem> {

    private static final long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static final int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    @Override
    protected void map(LogItem key, IntWritable value, Context context) throws IOException, InterruptedException {
        Long date = key.getDate();
        int periodNum = (int) ((date - startTime) / timePeriod);
        TimePeriodAndText extractKey = new TimePeriodAndText(timePeriod == 0 ? 0 : periodNum, new Text(key.getUri()));
//        System.out.println("==============>"+extractKey.getItem());
        String uriFolder = null;
        int secondSplitterIndex = extractKey.getItem().toString().indexOf("/", 1);
        if (secondSplitterIndex < 0)
            secondSplitterIndex = extractKey.getItem().toString().indexOf("?", 1);
        if (secondSplitterIndex < 0)
            secondSplitterIndex = extractKey.getItem().toString().length();
        uriFolder = extractKey.getItem().toString().substring(0, secondSplitterIndex);
//        uriFolder = extractKey.getItem().("/[^/]*$")[0];
//        try {
//            uriFolder = extractKey.getItem().split("/[^/]*$")[0];
//        } catch (Exception e) {
//            uriFolder = extractKey.getItem().split("\\?", 2)[0];
//        }
        extractKey.setItem(new Text(uriFolder));
        context.write(extractKey, key);
    }
}
