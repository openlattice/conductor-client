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

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class EntityKeyStreamSerializer implements SelfRegisteringStreamSerializer<EntityKey> {

    @Override
    public Class<? extends EntityKey> getClazz() {
        return EntityKey.class;
    }

    @Override
    public void write( ObjectDataOutput out, EntityKey object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public EntityKey read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.ENTITY_KEY.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, EntityKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getEntitySetId() );
        out.writeUTF( object.getEntityId() );
    }

    public static EntityKey deserialize( ObjectDataInput in ) throws IOException {
        final UUID entitySetId = UUIDStreamSerializer.deserialize( in );
        final String entityId = in.readUTF();
        return new EntityKey( entitySetId, entityId );
    }

}
