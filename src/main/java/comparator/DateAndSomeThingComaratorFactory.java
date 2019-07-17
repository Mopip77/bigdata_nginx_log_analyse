package comparator;

import model.TimePeriodAndSomething;
import model.TimePeriodAndText;
import model.TimePeriodAndTwoInteger;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import sun.util.cldr.CLDRLocaleDataMetaInfo;

public class DateAndSomeThingComaratorFactory {

    static class BaseComparator<T extends TimePeriodAndSomething> extends WritableComparator {

        public BaseComparator(Class model) {
            super(model, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            T a1 = (T) a;
            T b1 = (T) b;
            return a1.getItem().compareTo(b1.getItem());
        }
    }

    private static class TextComparator extends BaseComparator<TimePeriodAndText> {
        public TextComparator() {
            super(TimePeriodAndText.class);
        }
    }

    private static class TextReverseComparator extends BaseComparator<TimePeriodAndText> {
        public TextReverseComparator() {
            super(TimePeriodAndText.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    private static class TwoIntegerComparator extends BaseComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerComparator() {
            super(TimePeriodAndTwoInteger.class);
        }
    }

    private static class TwoIntegerReverseComparator extends BaseComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerReverseComparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static Class getComparator(Class model, boolean inverserCompare) {
        if (model == TimePeriodAndText.class)
            return inverserCompare ? TextReverseComparator.class : TextComparator.class;


        if (model == TimePeriodAndTwoInteger.class)
            return inverserCompare ? TwoIntegerReverseComparator.class : TwoIntegerComparator.class;

        return null;
    }
}
