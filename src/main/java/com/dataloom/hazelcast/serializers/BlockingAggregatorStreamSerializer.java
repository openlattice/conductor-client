package com.dataloom.hazelcast.serializers;

import com.dataloom.blocking.BlockingAggregator;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.HazelcastBlockingService;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Component
public class BlockingAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<BlockingAggregator> {

    private HazelcastBlockingService blockingService;

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

        out.writeInt( object.getPropertyTypesIndexedByFqn().size() );
        for ( Map.Entry<FullQualifiedName, UUID> entry : object.getPropertyTypesIndexedByFqn().entrySet() ) {
            FullQualifiedNameStreamSerializer.serialize( out, entry.getKey() );
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

        Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn = Maps.newHashMap();
        int fqnMapSize = in.readInt();
        for ( int i = 0; i < fqnMapSize; i++ ) {
            FullQualifiedName fqn = FullQualifiedNameStreamSerializer.deserialize( in );
            UUID id = UUIDStreamSerializer.deserialize( in );
            propertyTypeIdIndexedByFqn.put( fqn, id );
        }

        return new BlockingAggregator( graphId, entitySetIdsToSyncIds, propertyTypeIdIndexedByFqn, blockingService );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.BLOCKING_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {
    }

    public synchronized void setBlockingService( HazelcastBlockingService blockingService ) {
        Preconditions.checkState( this.blockingService == null, "HazelcastBlockingService can only be set once" );
        this.blockingService = Preconditions.checkNotNull( blockingService );
    }
}
