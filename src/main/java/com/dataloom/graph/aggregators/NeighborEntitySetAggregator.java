package com.dataloom.graph.aggregators;

import com.dataloom.graph.core.objects.NeighborTripletSet;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.google.common.collect.Sets;
import com.hazelcast.aggregation.Aggregator;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;

import java.util.Map;

public class NeighborEntitySetAggregator extends Aggregator<Map.Entry<EdgeKey, LoomEdge>, NeighborTripletSet> {

    private NeighborTripletSet edgeTriplets;

    public NeighborEntitySetAggregator() {
        this.edgeTriplets = new NeighborTripletSet( Sets.newHashSet() );
    }

    public NeighborEntitySetAggregator( NeighborTripletSet edgeTriplets ) {
        this.edgeTriplets = edgeTriplets;
    }

    @Override public void accumulate( Map.Entry<EdgeKey, LoomEdge> input ) {
        DelegatedUUIDList edgeTriplet = new DelegatedUUIDList(
                input.getValue().getSrcSetId(),
                input.getValue().getEdgeSetId(),
                input.getValue().getDstSetId()
        );
        edgeTriplets.add( edgeTriplet );
    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof NeighborEntitySetAggregator ) {
            edgeTriplets.addAll( ( (NeighborEntitySetAggregator) aggregator ).edgeTriplets );
        }
    }

    @Override public NeighborTripletSet aggregate() {
        return edgeTriplets;
    }

    public NeighborTripletSet getEdgeTriplets() {
        return edgeTriplets;
    }
}
