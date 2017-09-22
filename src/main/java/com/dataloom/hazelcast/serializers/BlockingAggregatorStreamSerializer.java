package com.dataloom.hazelcast.serializers;

import com.dataloom.blocking.BlockingAggregator;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Component
public class BlockingAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<BlockingAggregator> {
    private ConductorElasticsearchApi api;

    @Override public Class<? extends BlockingAggregator> getClazz() {
        return BlockingAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, BlockingAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );

        out.writeInt( object.getEntitySetIdsToSyncIds().size() );
        for ( Map.Entry<UUID, UUID> entry : object.getEntitySetIdsToSyncIds().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            UUIDStreamSerializer.serialize( out, entry.getValue() );
        }
    }

    @Override public BlockingAggregator read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );

        Map<UUID, UUID> entitySetIdsToSyncIds = Maps.newHashMap();
        int esMapSize = in.readInt();
        for ( int i = 0; i < esMapSize; i++ ) {
            UUID entitySetId = UUIDStreamSerializer.deserialize( in );
            UUID syncId = UUIDStreamSerializer.deserialize( in );
            entitySetIdsToSyncIds.put( entitySetId, syncId );
        }

        return new BlockingAggregator( graphId, entitySetIdsToSyncIds, api );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.BLOCKING_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {
    }

    public synchronized void setConductorElasticsearchApi( ConductorElasticsearchApi api ) {
        Preconditions.checkState( this.api == null, "Api can only be set once" );
        this.api = Preconditions.checkNotNull( api );
    }
}
