package com.dataloom.linking.aggregators;

import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.hazelcast.aggregation.Aggregator;

import java.util.Map;

public class CountVerticesAggregator extends Aggregator<Map.Entry<LinkingVertexKey, LinkingVertex>, Integer>  {
    private int numVertices = 0;

    @Override public void accumulate( Map.Entry<LinkingVertexKey, LinkingVertex> input ) {
        numVertices++;
    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof CountVerticesAggregator ) {
            numVertices += ( (CountVerticesAggregator) aggregator ).numVertices;
        }
    }

    @Override public Integer aggregate() {
        return numVertices;
    }
}
