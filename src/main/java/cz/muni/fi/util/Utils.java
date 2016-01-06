package cz.muni.fi.util;

import cz.muni.fi.commons.LongTriplet;
import cz.muni.fi.commons.MapLongTripletValueComparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 * @author Martin Jelínek (xjeline5)
 */
public class Utils {
    
    public static SortedMap<String, LongTriplet> getSortedMapWithTripletComparator(Map<String, LongTriplet> source) {
        MapLongTripletValueComparator valueComparator = new MapLongTripletValueComparator(source);
        TreeMap<String, LongTriplet> result = new TreeMap<>(valueComparator);
        result.putAll(source);
        return result;
    }

    public static <K, V> SortedMap<K, V> putFirstEntries(int max, SortedMap<K, V> source) {
        int count = 0;
        TreeMap<K, V> target = new TreeMap<>(source.comparator());
        for (Map.Entry<K, V> entry : source.entrySet()) {
            if (count >= max) {
                break;
            }

            target.put(entry.getKey(), entry.getValue());
            count++;
        }
        return target;
    }
    
    public static String mapWithTripletToCsv(Map<String, LongTriplet> source, int minCount) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, LongTriplet> entrySet : source.entrySet()) {
            sb.append(String.format("%s;%s;%s;%s;", entrySet.getKey(),
                    entrySet.getValue().getA().toString(),
                    entrySet.getValue().getB().toString(),
                    entrySet.getValue().getB().toString()));
        }
        if (source.size() < minCount) {
            for (int i = source.size(); i < minCount; i++) {
                sb.append(";;;;");
            }
        }
        return sb.toString();
    }
}
