package reducer;

import channel.NextStepMessageList;
import channel.NextStepMessageMap;
import model.TimePeriodAndText;
import model.TimePeriodAndTwoInteger;
import model.TwoInteger;
import myenum.StaticSortMainkey;
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
            StaticSortMainkey mainkey = context.getConfiguration().getEnum("mainkey", StaticSortMainkey.IPCOUNT);
            context.write(new Text("sort by [" + mainkey.getName() + "]\n-------------------"), null);
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
