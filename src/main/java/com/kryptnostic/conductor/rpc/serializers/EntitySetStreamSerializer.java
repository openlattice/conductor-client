package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class EntitySetStreamSerializer implements SelfRegisteringStreamSerializer<EntitySet> {

    @Override
    public void write( ObjectDataOutput out, EntitySet object )
            throws IOException {
        out.writeUTF( object.getName() );
        new FullQualifiedNameStreamSerializer().write( out, object.getType() );
        out.writeUTF( object.getTitle() );
    }

    @Override
    public EntitySet read( ObjectDataInput in ) throws IOException {
        EntitySet es = new EntitySet()
                .setName( in.readUTF() )
                .setType( new FullQualifiedNameStreamSerializer().read( in ) )
                .setTitle( in.readUTF() );
        return es;
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_SET.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<EntitySet> getClazz() {
        return EntitySet.class;
    }

}
