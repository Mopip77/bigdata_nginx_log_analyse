package reducer;

import model.TimePeriodAndText;
import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<TimePeriodAndText, LogItem, Text, IntWritable> {
    @Override
    protected void reduce(TimePeriodAndText key, Iterable<LogItem> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (LogItem value : values) {
            i += 1;
        }
        context.write(new Text(key.getItem()), new IntWritable(i));
    }
}
