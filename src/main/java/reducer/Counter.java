package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Counter extends Reducer<Object, Object, Object, IntWritable> {
    @Override
    protected void reduce(Object key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Object value : values) {
            sum+=1;
        }
        context.write(key, new IntWritable(sum));
    }
}
