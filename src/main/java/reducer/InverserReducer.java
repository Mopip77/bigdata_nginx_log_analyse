package reducer;

import channel.NextStepMessageMap;
import model.ThreeInteger;
import model.TimePeriodAndText;
import myenum.SortMainkey;
import myenum.VFSortMainkey;
import myenum.VIFSortMainkey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InverserReducer extends Reducer<Object, TimePeriodAndText, Text, Object> {

    private boolean flag = false;

    protected void initHeader(Context context) throws IOException, InterruptedException {
        TimePeriodAndText currentValue = context.getCurrentValue();
        if (currentValue != null) {
            String formerMessage = NextStepMessageMap.getInstance().read(currentValue.getPeriod());
            context.write(new Text(formerMessage), null);

            String mainkey = context.getConfiguration().get("mainkey");
            context.write(new Text("sort by [" + mainkey + "]\n-------------------"), null);
            }
    }

    @Override
    protected void reduce(Object key, Iterable<TimePeriodAndText> values, Context context) throws IOException, InterruptedException {
        if (!flag) {
            initHeader(context);
            flag = true;
        }
        Iterator<TimePeriodAndText> iterator = values.iterator();
        while (iterator.hasNext())
            context.write(iterator.next().getItem(), key);
    }
}
