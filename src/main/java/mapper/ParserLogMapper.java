package mapper;

import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.lf5.viewer.LogTable;
import sun.rmi.runtime.Log;

import javax.annotation.RegEx;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

    private static int startTime;
    private static int endTime;
    private static int timePeriod;

    static {
        // 读取startTime, endTime, timePeriod的配置文件
        startTime = 1427709094;
        endTime = 1427713294;
        timePeriod = 0;
    }

    /**
     * 传入log的时间，并且startTime必须不为0，否则不必filter
     * @param logDate
     */
    private Long logTimeFilter(String logDate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH);
        try {
            Date date = simpleDateFormat.parse(logDate);
            long dateTime = date.getTime() / 1000;
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

//            StringBuilder sb = new StringBuilder();
//            sb.append(ip).append(LOG_SPLITTER)
//                    .append(date).append(LOG_SPLITTER)
//                    .append(uri).append(LOG_SPLITTER)
//                    .append(statusCode).append(LOG_SPLITTER)
//                    .append(flux);
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