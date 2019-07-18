package reducer;

import channel.NextStepMessageMap;
import model.LogItem;
import model.TimePeriodAndText;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import properties.MyProperties;

import javax.swing.plaf.ViewportUI;
import java.io.IOException;
import java.util.Map;

public class IpStaticsReducer extends Reducer<TimePeriodAndText, LogItem, TimePeriodAndText, IntWritable> {
    private long totalIpCount = 0;
    private int partition;
    private boolean flag = false;
    private String info;

    @Override
    protected void reduce(TimePeriodAndText key, Iterable<LogItem> values, Context context) throws IOException, InterruptedException {
        if (!flag) {
            Map.Entry<Integer, String> header = ReduceUtils.getHeader(context);
            partition = header.getKey();
            info = header.getValue();
            flag = true;
        }

        int visitedCount = 0;
        for (LogItem value : values) {
            visitedCount++;
        }

        totalIpCount++;
        context.write(new TimePeriodAndText(partition, key.getItem()), new IntWritable(visitedCount));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String endInfo = "\ntotalIp: " + totalIpCount + "\n------------------";
//        context.write(new TimePeriodAndText(null, new Text(endInfo)), null);
        NextStepMessageMap.getInstance().write(partition, info+endInfo);
    }
}
