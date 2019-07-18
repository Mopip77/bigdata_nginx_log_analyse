package reducer;

import channel.NextStepMessageMap;
import model.TimePeriodAndText;
import model.LogItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class UriStaticsReducer extends Reducer<TimePeriodAndText, LogItem, TimePeriodAndText, Text> {
    private long totalIpCount = 0;
    private long totalUriCount = 0;
    private long totalVisitedCount = 0;
    private long totalFluxCount = 0;
    private String info;
    private Integer partition;
    private Boolean flag = false;

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
            Map.Entry<Integer, String> header = ReduceUtils.getHeader(context);
            partition = header.getKey();
            info = header.getValue();
            flag = true;
        }

        Set uniqueIpSet = new HashSet();
        int uriVisitedCount = 0;
        int fluxCount = 0;
        for (LogItem value : values) {
            uriVisitedCount += 1;
            uniqueIpSet.add(value.getIp());
            fluxCount += value.getFlux();
        }

        totalIpCount += uniqueIpSet.size();
        totalUriCount += 1;
        totalVisitedCount += uriVisitedCount;
        totalFluxCount += fluxCount;

        context.write(key, new Text(uriVisitedCount + " " + uniqueIpSet.size() + " " + fluxCount));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String endInfo = "\ntotalIpCount: " + totalIpCount + " totalUriCount: " + totalUriCount + " totalVisitedCount: " + totalVisitedCount + " totalFluxCount: " + totalFluxCount + "\n------------------";
        NextStepMessageMap.getInstance().write(partition, "\n"+info+endInfo);
//        context.write(null, new Text(endInfo));
    }
}
