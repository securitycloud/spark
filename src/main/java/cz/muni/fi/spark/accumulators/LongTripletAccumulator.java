package cz.muni.fi.spark.accumulators;

import cz.muni.fi.commons.LongTriplet;
import java.io.Serializable;
import org.apache.spark.AccumulatorParam;

/**
 * Accumulator implementation for LongTriplet
 * 
 * @author Martin Jelínek (xjeline5)
 */
public class LongTripletAccumulator implements AccumulatorParam<LongTriplet>, Serializable {

    @Override
    public LongTriplet addAccumulator(LongTriplet t, LongTriplet t1) {
        return t.add(t1);
    }

    @Override
    public LongTriplet addInPlace(LongTriplet r, LongTriplet r1) {
        return r.add(r1);
    }

    @Override
    public LongTriplet zero(LongTriplet r) {
        return new LongTriplet();
    }
    
}
