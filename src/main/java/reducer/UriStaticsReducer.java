package reducer;

import channel.NextStepMessage;
import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import properties.MyProperties;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class UriStaticsReducer extends Reducer<DateAndString, LogItem, Text, Text> {
    private long totalIpCount = 0;
    private long totalUriCount = 0;
    private long totalVisitedCount = 0;

    private static String  startTime = (String) MyProperties.getInstance().getPro().get("startTime");
    private static String  endTime =  (String) MyProperties.getInstance().getPro().get("endTime");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String info = "---------------\nstartTime: " + startTime + "\nendTime: " + endTime + "\n----------------";
        NextStepMessage.getInstance().append(info);
        context.write(new Text(info), new Text());
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
    protected void reduce(DateAndString key, Iterable<LogItem> values, Context context) throws IOException, InterruptedException {
        Set uniqueIpSet = new HashSet();
        int uriVisitedCount = 0;
        for (LogItem value : values) {
            uriVisitedCount += 1;
            uniqueIpSet.add(value.getIp());
        }

        totalIpCount += uniqueIpSet.size();
        totalUriCount += 1;
        totalVisitedCount += uriVisitedCount;

        context.write(new Text((String) key.getItem()), new Text(uriVisitedCount + " " + uniqueIpSet.size()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String info = "totalIpCount: " + totalIpCount + " totalUriCount: " + totalUriCount + " totalVisitedCount: " + totalVisitedCount + "\n------------------";
        NextStepMessage.getInstance().append("\n"+info);
        context.write(new Text(info), new Text());
    }
}
