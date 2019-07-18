package mapper;

import model.TimePeriodAndText;
import model.LogItem;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import properties.MyProperties;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UriExtractor extends Mapper<LogItem, IntWritable, TimePeriodAndText, LogItem> {

    private static final long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static final int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    @Override
    protected void map(LogItem key, IntWritable value, Context context) throws IOException, InterruptedException {
        Long date = key.getDate();
        int periodNum = (int) ((date - startTime) / timePeriod);
        TimePeriodAndText extractKey = new TimePeriodAndText(timePeriod == 0 ? 0 : periodNum, new Text(key.getUri()));
        String uri = extractKey.getItem().toString();
        String uriFolder = getUriFolder(uri);
        if (uriFolder == null) return;

        extractKey.setItem(new Text(uriFolder));
        context.write(extractKey, key);
    }

    public static String getUriFolder(String uri) {
        // url 解析
        // 不是/i (i为\\w) 的都不统计
        if (!Pattern.compile("^/\\w").matcher(uri).find())
            return null;

        String uriFolder = null;
        // thread forum space格式特殊先解析一次
        Pattern pattern = Pattern.compile("^/(thread|forum|space)-");
        Matcher matcher = pattern.matcher(uri);
        if (matcher.find()) {
            uriFolder = "/" + matcher.group(1);
        } else {
            int secondSplitterIndex = uri.indexOf("/", 1);
            if (secondSplitterIndex < 0)
                secondSplitterIndex = uri.indexOf("?", 1);
            if (secondSplitterIndex < 0)
                secondSplitterIndex = uri.length();
            uriFolder = StringUtils.substring(uri, 0, secondSplitterIndex);
        }
        return uriFolder;
    }
}
