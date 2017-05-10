package com.dataloom.hazelcast.serializers;

import com.dataloom.graph.core.Neighborhood;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class NeighborhoodStreamSerializer implements SelfRegisteringStreamSerializer<Neighborhood> {

    @Override public Class<Neighborhood> getClazz() {
        return Neighborhood.class;
    }

    @Override public void write( ObjectDataOutput out, Neighborhood object ) throws IOException {
        out.writeInt( object.getNeighborhood().size() );
        for ( Map.Entry<UUID, Map<UUID, SetMultimap<UUID, UUID>>> e : object.getNeighborhood().entrySet() ) {
            AbstractUUIDStreamSerializer.serialize( out, e.getKey() );
            out.writeInt( e.getValue().size() );
            for ( Map.Entry<UUID, SetMultimap<UUID, UUID>> e1 : e.getValue().entrySet() ) {
                AbstractUUIDStreamSerializer.serialize( out, e1.getKey() );
                out.writeInt( e1.getValue().size() );
                for ( Map.Entry<UUID, UUID> e2 : e1.getValue().entries() ) {
                    AbstractUUIDStreamSerializer.serialize( out, e2.getKey() );
                    AbstractUUIDStreamSerializer.serialize( out, e2.getValue() );
                }
            }
        }
    }

    @Override public Neighborhood read( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> m = new HashMap<>( size );
        for ( int i = 0; i < size; ++i ) {
            UUID id1 = AbstractUUIDStreamSerializer.deserialize( in );
            int size1 = in.readInt();
            Map<UUID, SetMultimap<UUID, UUID>> m1 = new HashMap<>( size1 );
            m.put( id1, m1 );
            for ( int j = 0; j < size1; ++j ) {
                UUID id2 = AbstractUUIDStreamSerializer.deserialize( in );
                int size2 = in.readInt();
                SetMultimap<UUID, UUID> m3 = HashMultimap.create();
                m1.put( id2, m3 );
                for ( int k = 0; k < size2; ++k ) {
                    UUID key = AbstractUUIDStreamSerializer.deserialize( in );
                    UUID v = AbstractUUIDStreamSerializer.deserialize( in );
                    m3.put( key, v );
                }
            }
        }
        return new Neighborhood( m );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.NEIGHBORHOOD_STREAM_SERIALIZER.ordinal();
    }

    @Override public void destroy() {

    }
}
