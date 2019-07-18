package reducer;

import channel.NextStepMessageMap;
import model.TimePeriodAndSomething;
import model.TimePeriodAndText;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import properties.MyProperties;

import javax.swing.text.rtf.RTFEditorKit;
import java.io.IOException;
import java.util.Map;

public abstract class ReduceUtils {

    private static long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
    private static long endTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("endTimeTimeStamp"));
    private static int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));

    public static <T extends TimePeriodAndSomething> Map.Entry<Integer, String> getHeader(Reducer.Context context) throws IOException, InterruptedException {
        T currentKey = (T) context.getCurrentKey();
        if (currentKey != null) {
            int partition = currentKey.getPeriod();
            long segmentStartTime = startTime + timePeriod * partition;
            long stopTime = Math.min(endTime, segmentStartTime + timePeriod);
            return new ImmutablePair<>(partition, "---------------\nstartTime: " + MyProperties.getInstance().formatTime(segmentStartTime) + "\nendTime: " + MyProperties.getInstance().formatTime(stopTime) + "\n----------------");
        } else {
            return null;
        }
    }
}
