package reducer;

import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class UriStaticsReducer extends Reducer<DateAndString, LogItem, Text, Text> {
    private long totalIpCount = 0;
    private long totalUriCount = 0;
    private long totalVisitedCount = 0;

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
        context.write(new Text("totalIpCount: " + totalIpCount + " totalUriCount: " + totalUriCount + " totalVisitedCount: " + totalVisitedCount),
                new Text("\n------------------"));
    }
}
