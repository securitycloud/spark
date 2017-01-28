package cz.muni.fi.spark.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.commons.Flow;
import cz.muni.fi.commons.LongTriplet;
import cz.muni.fi.spark.accumulators.BasicStatisticsAccumulators;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

/**
 * Class for basic NetFlow statistics processing
 * 
 * @author Martin Jelínek (xjeline5)
 */
public class BasicStatistics implements VoidFunction<JavaPairRDD<String, String>> {
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private final LongAccumulator processedRecordsCounter;
    private final BasicStatisticsAccumulators accumulators;

    public BasicStatistics(LongAccumulator processedRecordsCounter, BasicStatisticsAccumulators accumulators) {
        this.processedRecordsCounter = processedRecordsCounter;
        this.accumulators = accumulators;
    }

    @Override
    public void call(JavaPairRDD<String, String> rdd) throws Exception {
        rdd.foreachPartition((Iterator<Tuple2<String, String>> t) -> {
            int i = 0;
            HashMap<String, LongTriplet> ipMap = new HashMap<>();
            HashMap<String, LongTriplet> portMap = new HashMap<>();
            HashMap<String, LongTriplet> ipAggrMap = new HashMap<>();
            while (t.hasNext()) {
                Tuple2<String, String> msg = t.next();
                Flow flow = mapper.readValue(msg._2(), Flow.class);
                
                processedRecordsCounter.add(1);
                LongTriplet triplet = new LongTriplet(1L, (long)flow.getPackets(), (long)flow.getBytes());
                accumulators.getTotalCounter().add(triplet);
                
                if (!ipMap.containsKey(flow.getDst_ip_addr())) {
                    ipMap.put(flow.getDst_ip_addr(), triplet);
                } else {
                    LongTriplet existing = ipMap.get(flow.getDst_ip_addr());
                    ipMap.put(flow.getDst_ip_addr(), existing.add(triplet));
                }
                
                if (!portMap.containsKey(String.valueOf(flow.getDst_port()))) {
                    portMap.put(String.valueOf(flow.getDst_port()), triplet);
                } else {
                    LongTriplet existing = portMap.get(String.valueOf(flow.getDst_port()));
                    portMap.put(String.valueOf(flow.getDst_port()), existing.add(triplet));
                }
                
                if (!flow.getDst_ip_addr().contains(":")) {
                    String aggrIp = String.format("%s.*", flow.getDst_ip_addr().substring(0, flow.getDst_ip_addr().lastIndexOf(".")));
                    if (!ipAggrMap.containsKey(aggrIp)) {
                        ipAggrMap.put(aggrIp, triplet);
                    } else {
                        LongTriplet existing = ipAggrMap.get(aggrIp);
                        ipAggrMap.put(aggrIp, existing.add(triplet));
                    }
                }
                
                i++;
                if (i == 10000) {
                    accumulators.getPortCounter().add(filterMap(portMap, 3));
                    accumulators.getIpCounter().add(filterMap(ipMap, 3));
                    accumulators.getIpAggregationCounter().add(filterMap(ipAggrMap, 3));
                    portMap.clear();
                    ipMap.clear();
                    ipAggrMap.clear();
                    i = 0;
                }
            }
            
            accumulators.getPortCounter().add(filterMap(portMap, 3));
            accumulators.getIpCounter().add(filterMap(ipMap, 3));
            accumulators.getIpAggregationCounter().add(filterMap(ipAggrMap, 3));
        });
        
    }
    
    private static Map<String, LongTriplet> filterMap(Map<String, LongTriplet> map, Integer value) {
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, LongTriplet> pair = (Map.Entry<String, LongTriplet>) it.next();
            if (pair.getValue().getA() <= value) {
                it.remove(); // avoids a ConcurrentModificationException
            }
        }
        return map;
    }
}
