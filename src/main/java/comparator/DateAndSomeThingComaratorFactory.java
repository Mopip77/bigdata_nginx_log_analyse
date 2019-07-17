package comparator;

import model.TimePeriodAndSomething;
import model.TimePeriodAndText;
import model.TimePeriodAndTwoInteger;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.kerby.kerberos.kerb.client.request.AsRequestWithToken;
import sun.util.cldr.CLDRLocaleDataMetaInfo;

public class DateAndSomeThingComaratorFactory {

    private static String className = DateAndSomeThingComaratorFactory.class.getName();

    static class BaseComparator<T extends TimePeriodAndSomething> extends WritableComparator {

        public BaseComparator(Class model) {
            super(model, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            T a1 = (T) a;
            T b1 = (T) b;
            int firstCompare = a1.getPeriod().compareTo(b1.getPeriod());
            if (firstCompare != 0)
                return firstCompare;
            else
                return a1.getItem().compareTo(b1.getItem());
        }
    }

    static class BaseSubkeyComparator<T extends TimePeriodAndSomething> extends WritableComparator {

        public BaseSubkeyComparator(Class model) {
            super(model, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            T a1 = (T) a;
            T b1 = (T) b;
            int firstCompare = a1.getItem().compareTo(b1.getItem());
            if (firstCompare != 0)
                return firstCompare;
            else
                return a1.getPeriod().compareTo(b1.getPeriod());
        }
    }

    private static class TextComparator extends BaseComparator<TimePeriodAndText> {
        public TextComparator() {
            super(TimePeriodAndText.class);
        }
    }

    private static class TextSubkeyComparator extends BaseSubkeyComparator<TimePeriodAndText> {
        public TextSubkeyComparator() {
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

    private static class TextSubkeyReverseComparator extends BaseSubkeyComparator<TimePeriodAndText> {
        public TextSubkeyReverseComparator() {
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

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoDimensionComarator.compare(a1.getItem().getA1(), a1.getItem().getA2(), b1.getItem().getA1(), b1.getItem().getA2(),
                    false, false);
        }
    }

    private static class TwoIntegerSubkeyComparator extends BaseSubkeyComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerSubkeyComparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoDimensionComarator.compare(a1.getItem().getA1(), a1.getItem().getA2(), b1.getItem().getA1(), b1.getItem().getA2(),
                    false, true);
        }
    }

    private static class TwoIntegerReverseComparator extends BaseComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerReverseComparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoDimensionComarator.compare(a1.getItem().getA1(), a1.getItem().getA2(), b1.getItem().getA1(), b1.getItem().getA2(),
                    true, false);
        }
    }

    private static class TwoIntegerSubkeyReverseComparator extends BaseSubkeyComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerSubkeyReverseComparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoDimensionComarator.compare(a1.getItem().getA1(), a1.getItem().getA2(), b1.getItem().getA1(), b1.getItem().getA2(),
                    true, true);
        }
    }

    public static Class getComparator(Class model, boolean inverserCompare, boolean isSubkey) {
        String typeNameOfSomething = null;
        if (model == TimePeriodAndText.class)
            typeNameOfSomething = "Text";
        else if (model == TimePeriodAndTwoInteger.class)
            typeNameOfSomething = "TwoInteger";

        if (typeNameOfSomething == null)
            return null;
        String comparatorClassName = className + "$" + typeNameOfSomething + (isSubkey ? "Subkey" : "") + (inverserCompare ? "Reverse" : "") + "Comparator";
        try {
            return Class.forName(comparatorClassName);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
