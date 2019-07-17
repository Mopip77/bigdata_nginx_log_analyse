package partitioner;

import model.TimePeriodAndSomething;
import model.LogItem;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Partitioner;
import properties.MyProperties;

/**
 * 由于日志的处理可能是按照时间段分开处理，所以时间段分区时调用此partitioner，将按照时间段分区
 *  其中区号由 startTime timePeriod numReduceTasks决定
 *
 *  传入的是经过Extractor抽过的DateAndBasetype
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TimePeriodPartitioner extends Partitioner<TimePeriodAndSomething, LogItem> {
    // 配置文件读入
//    private static final long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static final int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    public int getPartition(TimePeriodAndSomething timePeriodAndSomething, LogItem logItem, int numPartitions) {
        return timePeriod == 0 ? 0 : timePeriodAndSomething.getPeriod() % numPartitions;
    }
}
