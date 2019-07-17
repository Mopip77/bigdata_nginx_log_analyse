package reducer;

import channel.NextStepMessage;
import model.TimePeriodAndText;
import model.LogItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import properties.MyProperties;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class UriStaticsReducer extends Reducer<TimePeriodAndText, LogItem, Text, Text> {
    private long totalIpCount = 0;
    private long totalUriCount = 0;
    private long totalVisitedCount = 0;
    private String info;

    private static String  startTime = (String) MyProperties.getInstance().getPro().get("startTime");
    private static String  endTime =  (String) MyProperties.getInstance().getPro().get("endTime");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        info = "---------------\nstartTime: " + startTime + "\nendTime: " + endTime + "\n----------------";
        context.write(null, new Text());
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
        Set uniqueIpSet = new HashSet();
        int uriVisitedCount = 0;
        for (LogItem value : values) {
            uriVisitedCount += 1;
            uniqueIpSet.add(value.getIp());
        }

        totalIpCount += uniqueIpSet.size();
        totalUriCount += 1;
        totalVisitedCount += uriVisitedCount;

        context.write(new Text(key.getItem()), new Text(uriVisitedCount + " " + uniqueIpSet.size()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String endInfo = "\ntotalIpCount: " + totalIpCount + " totalUriCount: " + totalUriCount + " totalVisitedCount: " + totalVisitedCount + "\n------------------";
        NextStepMessage.getInstance().append("\n"+info+endInfo);
        context.write(new Text(endInfo), new Text());
    }
}
