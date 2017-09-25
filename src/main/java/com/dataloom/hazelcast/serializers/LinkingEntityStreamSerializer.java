package com.dataloom.hazelcast.serializers;

import com.dataloom.blocking.LinkingEntity;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Component
public class LinkingEntityStreamSerializer implements SelfRegisteringStreamSerializer<LinkingEntity> {

    @Override public void write( ObjectDataOutput out, LinkingEntity object ) throws IOException {
        serialize( out, object );
    }

    @Override public LinkingEntity read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    public static void serialize( ObjectDataOutput out, LinkingEntity object ) throws IOException {
        Map<UUID, DelegatedStringSet> entity = object.getEntity();
        out.writeInt( entity.size() );
        for ( Map.Entry<UUID, DelegatedStringSet> entry : entity.entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            int setSize = entry.getValue().size();
            out.writeInt( setSize );
            for ( String str : entry.getValue() ) {
                out.writeUTF( str );
            }
        }
    }

    public static LinkingEntity deserialize( ObjectDataInput in ) throws IOException {
        Map<UUID, DelegatedStringSet> result = Maps.newHashMap();
        int mapSize = in.readInt();
        for ( int i = 0; i < mapSize; i++ ) {
            UUID id = UUIDStreamSerializer.deserialize( in );
            Set<String> strings = Sets.newHashSet();
            int setSize = in.readInt();
            for ( int j = 0; j < setSize; j++ ) {
                strings.add( in.readUTF() );
            }
            result.put( id, DelegatedStringSet.wrap( strings ) );
        }
        return new LinkingEntity( result );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.LINKING_ENTITY.ordinal();
    }

    @Override public void destroy() {
    }

    @Override public Class<? extends LinkingEntity> getClazz() {
        return LinkingEntity.class;
    }

}
