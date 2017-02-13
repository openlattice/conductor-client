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
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import org.springframework.stereotype.Component;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.Status;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class StatusStreamSerializer implements SelfRegisteringStreamSerializer<Status> {

    @Override
    public void write( ObjectDataOutput out, Status object ) throws IOException {
        PrincipalStreamSerializer.serialize( out, object.getPrincipal() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getAclKey() );
        DelegatedPermissionEnumSetStreamSerializer.serialize( out, object.getPermissions() );
        RequestStatusStreamSerializer.serialize( out, object.getStatus() );

    }

    @Override
    public Status read( ObjectDataInput in ) throws IOException {
        Principal principal = PrincipalStreamSerializer.deserialize( in );
        List<UUID> aclKey = ListStreamSerializers.fastUUIDListDeserialize( in );
        EnumSet<Permission> permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize( in );
        RequestStatus requestStatus = RequestStatusStreamSerializer.deserialize( in );
        return new Status( aclKey, principal, permissions, requestStatus );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.STATUS.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Status> getClazz() {
        return Status.class;
    }

}
