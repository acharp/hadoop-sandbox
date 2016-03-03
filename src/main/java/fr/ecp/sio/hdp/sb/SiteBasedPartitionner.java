package fr.ecp.sio.hdp.sb;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by charpi on 03/03/16.
 */
public class SiteBasedPartitionner extends Partitioner<CompositeKey,Measure> {

    @Override
    public int getPartition(CompositeKey compositeKey, Measure measure, int numReducer) {
        return Math.abs(compositeKey.siteId.hashCode()) % numReducer;
    }
}
