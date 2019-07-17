package mapper;

import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import properties.MyProperties;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析log文件， 返回<logItem，1>
 */
public class ParserLogMapper extends Mapper<LongWritable, Text, LogItem, IntWritable> {

    private static LogItem logItem = new LogItem();
    private static String LOG_SPLITTER = "-";

    private static long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static long endTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("endTimeTimeStamp"));
    private static int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    /**
     * 传入log的时间，并且startTime必须不为0，否则不必filter
     * @param logDate
     */
    private Long logTimeFilter(String logDate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH);
        try {
            Date date = simpleDateFormat.parse(logDate);
            Long dateTime = date.getTime() / 1000;
            return (dateTime >= startTime && dateTime <= endTime) ? dateTime : null;
        } catch (ParseException e) {
            System.out.println(e);
            return null;
        }
    }

    private LogItem parse(String log) {
        Pattern logPattern = Pattern.compile("^(.*) - - \\[(.* \\+\\d+)\\] \\\"\\w+ (.*) .*\\\" (\\d+) (-|\\d+)");
        Matcher matchers = logPattern.matcher(log);
        if (matchers.find()) {

            // 解析字段
            String dateStr = matchers.group(2);
            // 判断log时间是否在限定时间内
            Long date = null;
            if (startTime != 0) {
                date = logTimeFilter(dateStr);
                if (date == null)
                    return null;
            }

            String ip = matchers.group(1);
            String uri = matchers.group(3);
            Integer statusCode = Integer.valueOf(matchers.group(4));
            // flux 有的话为数字， 没有则为-
            String fluxPre = matchers.group(5);
            Integer flux = fluxPre.equals("-") ? 0 : Integer.valueOf(fluxPre);

            return new LogItem(ip, date, uri, statusCode, flux);
        }
        return null;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LogItem logItem = parse(value.toString());
        if (logItem != null)
            context.write(logItem, new IntWritable(1));
    }
}