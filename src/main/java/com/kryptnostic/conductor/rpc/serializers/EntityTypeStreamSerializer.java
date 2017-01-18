package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.EntityType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class EntityTypeStreamSerializer implements SelfRegisteringStreamSerializer<EntityType> {

    @Override
    public void write( ObjectDataOutput out, EntityType object ) throws IOException {
        AbstractUUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getSchemas(), ( FullQualifiedName schema ) -> {
            FullQualifiedNameStreamSerializer.serialize( out, schema );
        } );
        SetStreamSerializers.serialize( out, object.getKey(), ( UUID key ) -> {
            AbstractUUIDStreamSerializer.serialize( out, key );
        } );
        SetStreamSerializers.serialize( out, object.getProperties(), ( UUID property ) -> {
            AbstractUUIDStreamSerializer.serialize( out, property );
        } );
    }

    @Override
    public EntityType read( ObjectDataInput in ) throws IOException {
        UUID id = AbstractUUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return FullQualifiedNameStreamSerializer.deserialize( dataInput );
        } );
        Set<UUID> keys = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return AbstractUUIDStreamSerializer.deserialize( dataInput );
        } );
        Set<UUID> properties = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return AbstractUUIDStreamSerializer.deserialize( dataInput );
        } );
        return new EntityType( id, type, title, description, schemas, keys, properties );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<EntityType> getClazz() {
        return EntityType.class;
    }

}
