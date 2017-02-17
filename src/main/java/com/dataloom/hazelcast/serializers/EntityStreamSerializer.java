package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.linking.Entity;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EntityStreamSerializer implements SelfRegisteringStreamSerializer<Entity> {

    @Override
    public void write( ObjectDataOutput out, Entity object ) throws IOException {
        EntityKeyStreamSerializer.serialize( out, object.getKey() );
        //TODO Warning: write a map stream serializer?
        out.writeObject( object.getProperties() );
    }

    @Override
    public Entity read( ObjectDataInput in ) throws IOException {
        EntityKey ek = EntityKeyStreamSerializer.deserialize( in );
        //TODO Warning: write a map stream serializer?
        Map<String, Object> properties = in.readObject();
        return new Entity( ek, properties );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends Entity> getClazz() {
        return Entity.class;
    }

}
