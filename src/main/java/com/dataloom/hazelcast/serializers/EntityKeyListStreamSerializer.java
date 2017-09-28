package com.dataloom.hazelcast.serializers;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class EntityKeyListStreamSerializer implements SelfRegisteringStreamSerializer<EntityKey[]> {
    @Override public Class<? extends EntityKey[]> getClazz() {
        return EntityKey[].class;
    }

    @Override public void write( ObjectDataOutput out, EntityKey[] object ) throws IOException {
        out.writeInt( object.length );
        for (int i = 0; i < object.length; i++ ) {
            EntityKeyStreamSerializer.serialize( out, object[i] );
        }
    }

    @Override public EntityKey[] read( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        EntityKey[] result = new EntityKey[size];
        for (int i = 0; i < size; i++) {
            result[i] = EntityKeyStreamSerializer.deserialize( in );
        }
        return result;
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_KEY_ARRAY.ordinal();
    }

    @Override public void destroy() {

    }
}
