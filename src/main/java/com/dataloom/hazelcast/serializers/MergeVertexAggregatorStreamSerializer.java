package com.dataloom.hazelcast.serializers;

import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.HazelcastMergingService;
import com.dataloom.linking.aggregators.MergeVertexAggregator;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Component
public class MergeVertexAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<MergeVertexAggregator> {

    private HazelcastMergingService mergingService;

    @Override public Class<? extends MergeVertexAggregator> getClazz() {
        return MergeVertexAggregator.class;
    }

    @Override
    @SuppressFBWarnings
    public void write(
            ObjectDataOutput out, MergeVertexAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        UUIDStreamSerializer.serialize( out, object.getSyncId() );

        int ptByEsSize = object.getPropertyTypeIdsByEntitySet().size();
        out.writeInt( ptByEsSize );
        for ( Map.Entry<UUID, Set<UUID>> entry : object.getPropertyTypeIdsByEntitySet().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            SetStreamSerializers.fastUUIDSetSerialize( out, entry.getValue() );
        }

        int ptByIdSize = object.getPropertyTypesById().size();
        out.writeInt( ptByIdSize );
        for ( Map.Entry<UUID, PropertyType> entry : object.getPropertyTypesById().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            PropertyTypeStreamSerializer.serialize( out, entry.getValue() );
        }

        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTypesToPopulate() );

        int dtSize = object.getAuthorizedPropertiesWithDataTypeForLinkedEntitySet().size();
        out.writeInt( dtSize );
        for ( Map.Entry<UUID, EdmPrimitiveTypeKind> entry : object
                .getAuthorizedPropertiesWithDataTypeForLinkedEntitySet().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            EdmPrimitiveTypeKindStreamSerializer.serialize( out, entry.getValue() );
        }
    }

    @Override
    @SuppressFBWarnings
    public MergeVertexAggregator read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        UUID syncId = UUIDStreamSerializer.deserialize( in );

        Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet = Maps.newHashMap();
        int ptByEsSize = in.readInt();
        for ( int i = 0; i < ptByEsSize; i++ ) {
            UUID key = UUIDStreamSerializer.deserialize( in );
            Set<UUID> value = SetStreamSerializers.fastUUIDSetDeserialize( in );
            propertyTypeIdsByEntitySet.put( key, value );
        }

        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();
        int ptByIdSize = in.readInt();
        for ( int i = 0; i < ptByIdSize; i++ ) {
            UUID key = UUIDStreamSerializer.deserialize( in );
            PropertyType value = PropertyTypeStreamSerializer.deserialize( in );
            propertyTypesById.put( key, value );
        }

        Set<UUID> propertyTypesToPopulate = SetStreamSerializers.fastUUIDSetDeserialize( in );

        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet = Maps.newHashMap();
        int dtSize = in.readInt();
        for ( int i = 0; i < dtSize; i++ ) {
            UUID key = UUIDStreamSerializer.deserialize( in );
            EdmPrimitiveTypeKind value = EdmPrimitiveTypeKindStreamSerializer.deserialize( in );
            authorizedPropertiesWithDataTypeForLinkedEntitySet.put( key, value );
        }

        return new MergeVertexAggregator( graphId,
                syncId,
                propertyTypeIdsByEntitySet,
                propertyTypesById,
                propertyTypesToPopulate,
                authorizedPropertiesWithDataTypeForLinkedEntitySet,
                mergingService );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.MERGE_VERTEX_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }

    public synchronized void setMergingService( HazelcastMergingService mergingService ) {
        Preconditions.checkState( this.mergingService == null, "HazelcastMergingService can only be set once" );
        this.mergingService = Preconditions.checkNotNull( mergingService );
    }
}
