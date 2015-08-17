package cz.muni.fi.commons;

import java.util.Comparator;
import java.util.Map;

/**
 * Comparator by map value.
 */
public class MapValueComparator implements Comparator<String> {
    Map<String, Integer> base;

    public MapValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    @Override
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
