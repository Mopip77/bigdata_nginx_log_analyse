package reducer;

import channel.NextStepMessage;
import model.TimePeriodAndTwoInteger;
import model.TwoInteger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import properties.MyProperties;

import java.io.IOException;
import java.util.Iterator;

public class InverserReducer extends Reducer<TimePeriodAndTwoInteger, Text, Text, TwoInteger> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String formerMessage = NextStepMessage.getInstance().read();
        context.write(new Text(formerMessage), null);
    }

    @Override
    protected void reduce(TimePeriodAndTwoInteger key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext())
            context.write(iterator.next(), key.getItem());
    }
}
