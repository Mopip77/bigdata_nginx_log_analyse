package reducer;

import channel.NextStepMessageList;
import channel.NextStepMessageMap;
import model.TimePeriodAndTwoInteger;
import model.TwoInteger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InverserReducer extends Reducer<TimePeriodAndTwoInteger, Text, Text, TwoInteger> {

    private boolean flag = false;

    protected void initHeader(Context context) throws IOException, InterruptedException {
        TimePeriodAndTwoInteger currentKey = context.getCurrentKey();
        if (currentKey != null) {
            String formerMessage = NextStepMessageMap.getInstance().read(currentKey.getPeriod());
            context.write(new Text(formerMessage), null);
        }
    }

    @Override
    protected void reduce(TimePeriodAndTwoInteger key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (!flag) {
            initHeader(context);
            flag = true;
        }
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext())
            context.write(iterator.next(), key.getItem());
    }
}
