package reducer;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DirectReducer extends Reducer {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
