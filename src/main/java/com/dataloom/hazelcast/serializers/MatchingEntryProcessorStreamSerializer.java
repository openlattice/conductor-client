package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.dataloom.data.hazelcast.Entities;
import com.dataloom.matching.MatchingEntryProcessor;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.matching.MatchingAggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jersey.repackaged.com.google.common.collect.Maps;

@Component
public class MatchingEntryProcessorStreamSerializer implements SelfRegisteringStreamSerializer<MatchingEntryProcessor> {
    private ConductorElasticsearchApi api;

    @Override
    @SuppressFBWarnings
    public void write( ObjectDataOutput out, MatchingEntryProcessor object ) throws IOException {

        UUIDStreamSerializer.serialize( out, object.getGraphId() );

        out.writeInt( object.getEntitySetIdsToSyncIds().size() );
        for ( Map.Entry<UUID, UUID> entry : object.getEntitySetIdsToSyncIds().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            UUIDStreamSerializer.serialize( out, entry.getValue() );
        }

        out.writeInt( object.getAuthorizedPropertyTypes().size() );
        for ( Map.Entry<UUID, PropertyType> entry : object.getAuthorizedPropertyTypes().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            PropertyTypeStreamSerializer.serialize( out, entry.getValue() );
        }

        out.writeInt( object.getPropertyTypeIdIndexedByFqn().size() );
        for ( Map.Entry<FullQualifiedName, String> entry : object.getPropertyTypeIdIndexedByFqn().entrySet() ) {
            FullQualifiedNameStreamSerializer.serialize( out, entry.getKey() );
            out.writeUTF( entry.getValue() );
        }

        EntitiesStreamSerializer.serialize( out, object.getEntities() );
    }

    @Override
    @SuppressFBWarnings
    public MatchingEntryProcessor read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );

        Map<UUID, UUID> entitySetIdsToSyncIds = Maps.newHashMap();
        int esMapSize = in.readInt();
        for ( int i = 0; i < esMapSize; i++ ) {
            UUID entitySetId = UUIDStreamSerializer.deserialize( in );
            UUID syncId = UUIDStreamSerializer.deserialize( in );
            entitySetIdsToSyncIds.put( entitySetId, syncId );
        }

        Map<UUID, PropertyType> authorizedPropertyTypes = Maps.newHashMap();
        int ptMapSize = in.readInt();
        for ( int i = 0; i < ptMapSize; i++ ) {
            UUID id = UUIDStreamSerializer.deserialize( in );
            PropertyType pt = PropertyTypeStreamSerializer.deserialize( in );
            authorizedPropertyTypes.put( id, pt );
        }

        Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn = Maps.newHashMap();
        int fqnMapSize = in.readInt();
        for ( int i = 0; i < fqnMapSize; i++ ) {
            FullQualifiedName fqn = FullQualifiedNameStreamSerializer.deserialize( in );
            String id = in.readUTF();
            propertyTypeIdIndexedByFqn.put( fqn, id );
        }

        Entities entities = EntitiesStreamSerializer.deserialize( in );

        return new MatchingEntryProcessor(
                graphId,
                entitySetIdsToSyncIds,
                authorizedPropertyTypes,
                propertyTypeIdIndexedByFqn,
                entities,
                api );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.MATCHING_ENTRY_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends MatchingEntryProcessor> getClazz() {
        return MatchingEntryProcessor.class;
    }

    public synchronized void setConductorElasticsearchApi( ConductorElasticsearchApi api ) {
        Preconditions.checkState( this.api == null, "Api can only be set once" );
        this.api = Preconditions.checkNotNull( api );
    }

}
