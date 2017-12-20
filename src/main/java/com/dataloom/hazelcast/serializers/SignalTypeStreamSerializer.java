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

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.neuron.SignalType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class SignalTypeStreamSerializer implements SelfRegisteringStreamSerializer<SignalType> {

    private static final SignalType[] values = SignalType.values();

    @Override
    public void write( ObjectDataOutput out, SignalType object ) throws IOException {

        serialize( out, object );
    }

    @Override
    public SignalType read( ObjectDataInput in ) throws IOException {

        return deserialize( in );
    }

    @Override
    public int getTypeId() {

        return StreamSerializerTypeIds.SIGNAL_TYPE.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<SignalType> getClazz() {

        return SignalType.class;
    }

    public static void serialize( ObjectDataOutput out, SignalType object ) throws IOException {

        out.writeInt( object.ordinal() );
    }

    public static SignalType deserialize( ObjectDataInput in ) throws IOException {

        return values[ in.readInt() ];
    }

}
