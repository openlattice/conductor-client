package com.dataloom.hazelcast.serializers;

import com.dataloom.blocking.LoadingAggregator;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Component
public class LoadingAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<LoadingAggregator> {

    @Override public void write( ObjectDataOutput out, LoadingAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        Map<UUID, Map<UUID, PropertyType>> pts = object.getAuthorizedPropertyTypes();
        out.writeInt( pts.size() );
        for ( Map.Entry<UUID, Map<UUID, PropertyType>> entry : pts.entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            out.writeInt( entry.getValue().size() );
            for ( Map.Entry<UUID, PropertyType> valueEntry : entry.getValue().entrySet() ) {
                UUIDStreamSerializer.serialize( out, valueEntry.getKey() );
                PropertyTypeStreamSerializer.serialize( out, valueEntry.getValue() );
            }
        }
    }

    @Override public LoadingAggregator read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );

        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = Maps.newHashMap();

        int numEntries = in.readInt();
        for ( int i = 0; i < numEntries; i++ ) {
            UUID entitySetId = UUIDStreamSerializer.deserialize( in );
            Map<UUID, PropertyType> pts = Maps.newHashMap();
            int numValueEntries = in.readInt();
            for ( int j = 0; j < numValueEntries; j++ ) {
                UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
                PropertyType pt = PropertyTypeStreamSerializer.deserialize( in );
                pts.put( propertyTypeId, pt );
            }
            authorizedPropertyTypes.put( entitySetId, pts );
        }
        return new LoadingAggregator( graphId, authorizedPropertyTypes );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.LOADING_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {
    }

    @Override public Class<? extends LoadingAggregator> getClazz() {
        return LoadingAggregator.class;
    }
}
