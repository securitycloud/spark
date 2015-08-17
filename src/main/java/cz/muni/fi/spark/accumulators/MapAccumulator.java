package cz.muni.fi.spark.accumulators;

import org.apache.spark.AccumulatorParam;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulator implementation for Map<String, Integer>
 */
public class MapAccumulator implements AccumulatorParam<Map<String, Integer>>, Serializable {

    @Override
    public Map<String, Integer> addAccumulator(Map<String, Integer> t1, Map<String, Integer> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<String, Integer> addInPlace(Map<String, Integer> r1, Map<String, Integer> r2) {
        return mergeMap(r1, r2);
    }

    @Override
    public Map<String, Integer> zero(final Map<String, Integer> initialValue) {
        return new HashMap<>();
    }

    private Map<String, Integer> mergeMap(Map<String, Integer> map1, Map<String, Integer> map2) {
        Map<String, Integer> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> a + b));
        return result;
    }

}
