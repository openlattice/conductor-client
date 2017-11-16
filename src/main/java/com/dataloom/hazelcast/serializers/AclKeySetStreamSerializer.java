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

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AclKeySet;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;

@Component
public class AclKeySetStreamSerializer implements SelfRegisteringStreamSerializer<AclKeySet> {
    @Override public Class<? extends AclKeySet> getClazz() {
        return AclKeySet.class;
    }

    @Override public void write( ObjectDataOutput out, AclKeySet object ) throws IOException {
        SetStreamSerializers.serialize( out, object, aclKey -> {
            AclKeyStreamSerializer.serialize( out, aclKey );
        } );
    }

    @Override public AclKeySet read( ObjectDataInput in ) throws IOException {
        Set<AclKey> aclKeys = SetStreamSerializers.deserialize( in, AclKeyStreamSerializer::deserialize );

        return new AclKeySet( aclKeys );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.ACL_KEY_SET.ordinal();
    }

    @Override public void destroy() {

    }
}