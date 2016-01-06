package cz.muni.fi.commons;

import java.util.Comparator;
import java.util.Map;

/**
 * Comparator by map value - the first value of numeric triplet.
 */
public class MapLongTripletValueComparator implements Comparator<String> {
    Map<String, LongTriplet> base;

    public MapLongTripletValueComparator(Map<String, LongTriplet> base) {
        this.base = base;
    }

    @Override
    public int compare(String a, String b) {
        if (base.get(a).getA() >= base.get(b).getA()) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
