package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.AbstractSchemaAssociatedSecurableType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.serializers.FullQualifiedNameStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class TypePkStreamSerializer implements SelfRegisteringStreamSerializer<AbstractSchemaAssociatedSecurableType>{

    @Override
    public void write( ObjectDataOutput out, AbstractSchemaAssociatedSecurableType object ) throws IOException {
        FullQualifiedName fqn = object.getType();
        AbstractUUIDStreamSerializer.serialize( out, object.getId() );
        out.writeUTF( fqn.getNamespace() );
        out.writeUTF(  fqn.getName() );
        SetStreamSerializers.serialize( out, object.getSchemas(), FullQualifiedNameStreamSerializer::serialize );
    }

    @Override
    public AbstractSchemaAssociatedSecurableType read( ObjectDataInput in ) throws IOException {
        UUID id = AbstractUUIDStreamSerializer.deserialize( in );
        String namespace = in.readUTF();
        String name = in.readUTF();
        
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, FullQualifiedNameStreamSerializer::deserialize );
        return new AbstractSchemaAssociatedSecurableType( id,new FullQualifiedName( namespace, name ) , schemas );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.TYPE_PK.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<AbstractSchemaAssociatedSecurableType> getClazz() {
        return AbstractSchemaAssociatedSecurableType.class;
    }

}
