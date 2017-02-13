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

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class PrincipalRemoverStreamSerializer implements SelfRegisteringStreamSerializer<PrincipalRemover> {
    @Override public Class<PrincipalRemover> getClazz() {
        return PrincipalRemover.class;
    }

    @Override public void write(
            ObjectDataOutput out, PrincipalRemover object ) throws IOException {
        SetStreamSerializers.serialize( out,
                object.getBackingCollection(),
                elem -> PrincipalStreamSerializer.serialize( out, elem ) );

    }

    @Override public PrincipalRemover read( ObjectDataInput in ) throws IOException {
        return new PrincipalRemover( SetStreamSerializers.deserialize( in , PrincipalStreamSerializer::deserialize ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PRINCIPAL_REMOVER.ordinal();
    }

    @Override public void destroy() {

    }
}
