package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.HazelcastMergingService;
import com.dataloom.linking.aggregators.MergeEdgeAggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class MergeEdgeAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<MergeEdgeAggregator> {

    private HazelcastMergingService mergingService;

    @Override public Class<? extends MergeEdgeAggregator> getClazz() {
        return MergeEdgeAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, MergeEdgeAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getLinkedEntitySetId() );
        UUIDStreamSerializer.serialize( out, object.getSyncId() );
    }

    @Override public MergeEdgeAggregator read( ObjectDataInput in ) throws IOException {
        UUID linkedEntitySetId = UUIDStreamSerializer.deserialize( in );
        UUID syncId = UUIDStreamSerializer.deserialize( in );
        return new MergeEdgeAggregator( linkedEntitySetId, syncId, mergingService );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.MERGE_EDGE_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }

    public synchronized void setMergingService( HazelcastMergingService mergingService ) {
        Preconditions.checkState( this.mergingService == null, "HazelcastMergingService can only be set once" );
        this.mergingService = Preconditions.checkNotNull( mergingService );
    }
}
