package com.dataloom.hazelcast.serializers;

import com.dataloom.graph.aggregators.NeighborEntitySetAggregator;
import com.dataloom.graph.core.objects.NeighborTripletSet;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class NeighborEntitySetAggregatorStreamSerializer
        implements SelfRegisteringStreamSerializer<NeighborEntitySetAggregator> {
    @Override public Class<? extends NeighborEntitySetAggregator> getClazz() {
        return NeighborEntitySetAggregator.class;
    }

    @Override public void write(
            ObjectDataOutput out, NeighborEntitySetAggregator object ) throws IOException {
        NeighborTripletSetStreamSerializer.serialize( out, object.getEdgeTriplets() );
    }

    @Override public NeighborEntitySetAggregator read( ObjectDataInput in ) throws IOException {
        NeighborTripletSet edgeTriplets = NeighborTripletSetStreamSerializer.deserialize( in );
        return new NeighborEntitySetAggregator( edgeTriplets );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.NEIGHBOR_ENTITY_SET_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }
}
