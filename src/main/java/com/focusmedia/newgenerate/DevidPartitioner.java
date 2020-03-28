package com.focusmedia.newgenerate;

import org.apache.spark.Partitioner;

public class DevidPartitioner extends Partitioner {
    @Override
    public int numPartitions() {
        return 500;
    }

    @Override
    public int getPartition(Object key) {
        return key.hashCode() % numPartitions();
    }
}
