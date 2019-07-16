package reducer;

import model.DateAndString;
import model.LogItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TestReducer extends Reducer<DateAndString, LogItem, Text, Text> {
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
        context.write(new Text("haha"), new Text("bibi"));
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        context.write(new Text("totalIpCount: " + totalIpCount + " totalUriCount: " + totalUriCount + " totalVisitedCount: " + totalVisitedCount),
//                new Text("\n------------------"));
//    }
}
