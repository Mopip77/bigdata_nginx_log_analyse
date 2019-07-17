package comparator;

public class TwoDimensionComarator {
    public static int compare(Comparable key1, Comparable val1, Comparable key2, Comparable val2, boolean reverse, boolean subKeyPriority) {
        int result;
        if (!subKeyPriority) {
            int firstCompare = key1.compareTo(key2);
            if (firstCompare != 0)
                result = firstCompare;
            else
                result = val1.compareTo(val2);
        } else {
            int firstCompare = val1.compareTo(val2);
            if (firstCompare != 0)
                result = firstCompare;
            else
                result = key1.compareTo(key2);
        }
        return reverse ? - result : result;
    }
}
