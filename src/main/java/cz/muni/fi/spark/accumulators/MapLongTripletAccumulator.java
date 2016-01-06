package cz.muni.fi.spark.accumulators;

import cz.muni.fi.commons.LongTriplet;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.AccumulatorParam;

/**
 * Accumulator implementation for Map<String, LongTriplet>
 * 
 * @author Martin Jelínek (xjeline5)
 */
public class MapLongTripletAccumulator implements AccumulatorParam<Map<String, LongTriplet>>, Serializable {

    @Override
    public Map<String, LongTriplet> addAccumulator(Map<String, LongTriplet> t, Map<String, LongTriplet> t1) {
        return mergeMap(t, t1);
    }

    @Override
    public Map<String, LongTriplet> addInPlace(Map<String, LongTriplet> r, Map<String, LongTriplet> r1) {
        return mergeMap(r, r1);
    }

    @Override
    public Map<String, LongTriplet> zero(Map<String, LongTriplet> r) {
        return new HashMap<>();
    }
    
    private Map<String, LongTriplet> mergeMap(Map<String, LongTriplet> map1, Map<String, LongTriplet> map2) {
        map2.forEach((k, v) -> map1.merge(k, v, (a, b) -> a.add(b) ));
        return map1;
    }
    
}
