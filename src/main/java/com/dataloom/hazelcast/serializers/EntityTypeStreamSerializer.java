/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.type.EntityType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EntityTypeStreamSerializer implements SelfRegisteringStreamSerializer<EntityType> {

    @Override
    public void write( ObjectDataOutput out, EntityType object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out, object.getSchemas(), ( FullQualifiedName schema ) -> {
            FullQualifiedNameStreamSerializer.serialize( out, schema );
        } );
        SetStreamSerializers.serialize( out, object.getKey(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        SetStreamSerializers.serialize( out, object.getProperties(), ( UUID property ) -> {
            UUIDStreamSerializer.serialize( out, property );
        } );
    }

    @Override
    public EntityType read( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializer.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return FullQualifiedNameStreamSerializer.deserialize( dataInput );
        } );
        Set<UUID> keys = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return UUIDStreamSerializer.deserialize( dataInput );
        } );
        Set<UUID> properties = SetStreamSerializers.deserialize( in, new TreeSet<UUID>(), in.readInt(), ( ObjectDataInput dataInput ) -> {
            return UUIDStreamSerializer.deserialize( dataInput );
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
