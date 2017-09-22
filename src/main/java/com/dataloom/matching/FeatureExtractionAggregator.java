package com.dataloom.matching;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.hazelcast.aggregation.Aggregator;

import java.util.Map;

public class FeatureExtractionAggregator extends Aggregator<Map.Entry<GraphEntityPair, LinkingEntity>, Boolean> {
    private GraphEntityPair graphEntityPair;
   // private Map<GraphEntityPair, >

    @Override public void accumulate( Map.Entry<GraphEntityPair, LinkingEntity> input ) {

    }

    @Override public void combine( Aggregator aggregator ) {

    }

    @Override public Boolean aggregate() {
        return null;
    }
}
