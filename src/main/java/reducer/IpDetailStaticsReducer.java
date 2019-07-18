package reducer;

import channel.NextStepMessageMap;
import mapper.UriExtractor;
import model.LogItem;
import model.TimePeriodAndText;
import model.TwoInteger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import properties.MyProperties;

import java.io.IOException;
import java.util.*;

public class IpDetailStaticsReducer extends Reducer<TimePeriodAndText, LogItem, TimePeriodAndText, TwoInteger> {
    private long totalIpCount = 0;
    private long totalFluxCount = 0;
    private int partition;
    private boolean flag = false;
    private String info;
    private int soutUriCount = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("ipPrintUriCount"));

    @Override
    protected void reduce(TimePeriodAndText key, Iterable<LogItem> values, Context context) throws IOException, InterruptedException {
        if (!flag) {
            Map.Entry<Integer, String> header = ReduceUtils.getHeader(context);
            partition = header.getKey();
            info = header.getValue();
            flag = true;
        }

        Map<String, Integer> uriCount = new HashMap<>();
        int visitedCount = 0;
        int fluxCount = 0;
        for (LogItem value : values) {
            visitedCount++;
            fluxCount += value.getFlux();
            String uri = UriExtractor.getUriFolder(value.getUri());
            if (StringUtils.isNotBlank(uri)) {
                uriCount.put(uri, uriCount.getOrDefault(uri, 0) + 1);
            }
        }

        List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String, Integer>>(uriCount.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return -o1.getValue().compareTo(o2.getValue());
            }
        });

        totalIpCount++;
        totalFluxCount += fluxCount;

        for (int i = 0; i < Math.min(list.size(), soutUriCount); i++) {
            Map.Entry<String, Integer> mostVisitUri = list.remove(0);
            context.write(new TimePeriodAndText(partition, new Text(key.getItem().toString() + "___" + mostVisitUri.getKey() + "___" + mostVisitUri.getValue())), new TwoInteger(visitedCount, fluxCount));
        }
        context.write(null, null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String endInfo = "\ntotalIp: " + totalIpCount + "\n------------------";
//        context.write(new TimePeriodAndText(null, new Text(endInfo)), null);
        NextStepMessageMap.getInstance().write(partition, info+endInfo);
    }
}
