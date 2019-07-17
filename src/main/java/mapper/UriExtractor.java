package mapper;

import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import properties.MyProperties;

import java.io.IOException;
import java.util.regex.Pattern;

public class UriExtractor extends Mapper<LogItem, IntWritable, DateAndString, LogItem> {

    private static final int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    @Override
    protected void map(LogItem key, IntWritable value, Context context) throws IOException, InterruptedException {
        DateAndString extractKey = new DateAndString(timePeriod == 0 ? 0 : key.getDate(), key.getUri());
//        System.out.println("==============>"+extractKey.getItem());
        String uriFolder = null;
        int secondSplitterIndex = extractKey.getItem().indexOf("/", 1);
        if (secondSplitterIndex < 0)
            secondSplitterIndex = extractKey.getItem().indexOf("?", 1);
        if (secondSplitterIndex < 0)
            secondSplitterIndex = extractKey.getItem().length();
        uriFolder = extractKey.getItem().substring(0, secondSplitterIndex);
//        uriFolder = extractKey.getItem().("/[^/]*$")[0];
//        try {
//            uriFolder = extractKey.getItem().split("/[^/]*$")[0];
//        } catch (Exception e) {
//            uriFolder = extractKey.getItem().split("\\?", 2)[0];
//        }
        extractKey.setItem(uriFolder);
        context.write(extractKey, key);
    }
}
