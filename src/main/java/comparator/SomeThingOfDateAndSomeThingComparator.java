package comparator;

import model.TimePeriodAndSomething;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.sql.Time;

public class SomeThingOfDateAndSomeThingComparator extends WritableComparator {
    public SomeThingOfDateAndSomeThingComparator() {
        super(TimePeriodAndSomething.class, true);
        System.out.println(1);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TimePeriodAndSomething a1 = (TimePeriodAndSomething) a;
        TimePeriodAndSomething b1 = (TimePeriodAndSomething) b;
        return a1.getItem().compareTo(b1.getItem());
    }
}
