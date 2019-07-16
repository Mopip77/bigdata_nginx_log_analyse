package reducer;

import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.ws.handler.LogicalHandler;
import java.io.IOException;
import java.io.Reader;

public class CountReducer extends Reducer<DateAndString, LogItem, Text, IntWritable> {
    @Override
    protected void reduce(DateAndString key, Iterable<LogItem> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (LogItem value : values) {
            i += 1;
        }
        context.write(new Text(key.getItem()), new IntWritable(i));
    }
}
