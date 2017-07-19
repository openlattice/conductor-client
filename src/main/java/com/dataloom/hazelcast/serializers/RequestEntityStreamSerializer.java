/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.dataloom.hazelcast.serializers;

import com.dataloom.data.EntityKey;
import com.dataloom.data.requests.Entity;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import de.javakaffee.kryoserializers.UUIDSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class RequestEntityStreamSerializer implements SelfRegisteringStreamSerializer<Entity> {

    private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {

        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.register( UUID.class, new UUIDSerializer() );
            HashMultimapSerializer.registerSerializers( kryo );
            ImmutableMultimapSerializer.registerSerializers( kryo );
            return kryo;
        }
    };

    @Override
    public void write( ObjectDataOutput out, Entity object ) throws IOException {
        EntityKeyStreamSerializer.serialize( out, object.getKey() );
        Output output = new Output( (OutputStream) out );
        kryoThreadLocal.get().writeClassAndObject( output, object.getDetails() );
        output.flush();
    }

    @Override
    public Entity read( ObjectDataInput in ) throws IOException {
        EntityKey ek = EntityKeyStreamSerializer.deserialize( in );
        Input input = new Input( (InputStream) in );
        SetMultimap<UUID, Object> m = (SetMultimap<UUID, Object>) kryoThreadLocal.get().readClassAndObject( input );
        return new Entity( ek, m );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.REQUEST_ENTITY.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends Entity> getClazz() {
        return Entity.class;
    }

}
