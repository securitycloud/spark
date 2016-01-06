package cz.muni.fi.spark.accumulators;

import cz.muni.fi.commons.LongTriplet;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.Accumulator;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Accumulators for basic NetFlow statistics
 * 
 * @author Martin Jelínek (xjeline5)
 */
public class BasicStatisticsAccumulators implements Serializable {
    
    private final Accumulator<LongTriplet> totalCounter;
    private final Accumulator<Map<String, LongTriplet>> ipCounter;
    private final Accumulator<Map<String, LongTriplet>> portCounter;
    private final Accumulator<Map<String, LongTriplet>> ipAggregationCounter;
    
    public BasicStatisticsAccumulators(JavaStreamingContext streamingContext) {
        totalCounter = streamingContext.sparkContext().accumulator(new LongTriplet(), new LongTripletAccumulator());
        ipCounter = streamingContext.sparkContext().accumulator(new HashMap<String, LongTriplet>(), new MapLongTripletAccumulator());
        portCounter = streamingContext.sparkContext().accumulator(new HashMap<String, LongTriplet>(), new MapLongTripletAccumulator());
        ipAggregationCounter = streamingContext.sparkContext().accumulator(new HashMap<String, LongTriplet>(), new MapLongTripletAccumulator());
    }

    public Accumulator<LongTriplet> getTotalCounter() {
        return totalCounter;
    }

    public Accumulator<Map<String, LongTriplet>> getIpCounter() {
        return ipCounter;
    }

    public Accumulator<Map<String, LongTriplet>> getPortCounter() {
        return portCounter;
    }

    public Accumulator<Map<String, LongTriplet>> getIpAggregationCounter() {
        return ipAggregationCounter;
    }
    
}
