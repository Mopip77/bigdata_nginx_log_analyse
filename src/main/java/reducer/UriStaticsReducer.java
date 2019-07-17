package reducer;

import channel.NextStepMessageList;
import channel.NextStepMessageMap;
import model.TimePeriodAndText;
import model.LogItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import properties.MyProperties;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class UriStaticsReducer extends Reducer<TimePeriodAndText, LogItem, TimePeriodAndText, Text> {
    private long totalIpCount = 0;
    private long totalUriCount = 0;
    private long totalVisitedCount = 0;
    private String info;
    private Integer partition;
    private Boolean flag = false;

    private static long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static long endTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("endTimeTimeStamp"));
    private static int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    protected void initHeader(Context context) throws IOException, InterruptedException {
        TimePeriodAndText currentKey = context.getCurrentKey();
        if (currentKey != null) {
            partition = currentKey.getPeriod();
            long segmentStartTime = startTime + timePeriod * partition;
            info = "---------------\nstartTime: " + MyProperties.getInstance().formatTime(segmentStartTime) + "\nendTime: " + MyProperties.getInstance().formatTime(segmentStartTime + timePeriod) + "\n----------------";
            context.write(null, new Text(info));
        }
    }

    /**
     * 经过group，key为<date:long, uri:string> value为uri和date都属于这个key的LogItem
     * return <uri, <pv, ip>>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(TimePeriodAndText key, Iterable<LogItem> values, Context context) throws IOException, InterruptedException {
        if (!flag) {
            initHeader(context);
            flag = true;
        }

        Set uniqueIpSet = new HashSet();
        int uriVisitedCount = 0;
        for (LogItem value : values) {
            uriVisitedCount += 1;
            uniqueIpSet.add(value.getIp());
        }

        totalIpCount += uniqueIpSet.size();
        totalUriCount += 1;
        totalVisitedCount += uriVisitedCount;

        context.write(key, new Text(uriVisitedCount + " " + uniqueIpSet.size()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String endInfo = "\ntotalIpCount: " + totalIpCount + " totalUriCount: " + totalUriCount + " totalVisitedCount: " + totalVisitedCount + "\n------------------";
        NextStepMessageMap.getInstance().write(partition, "\n"+info+endInfo);
        context.write(null, new Text(endInfo));
    }
}
