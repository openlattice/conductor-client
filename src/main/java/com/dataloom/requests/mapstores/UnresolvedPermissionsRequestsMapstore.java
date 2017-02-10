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

package com.dataloom.requests.mapstores;

import java.util.UUID;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.RequestStatus;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.reflect.TypeToken;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class UnresolvedPermissionsRequestsMapstore
        extends AbstractStructuredCassandraMapstore<AclRootPrincipalPair, PermissionsRequestDetails> {
    public UnresolvedPermissionsRequestsMapstore( Session session ) {
        super(
                HazelcastMap.PERMISSIONS_REQUESTS_UNRESOLVED.name(),
                session,
                Tables.PERMISSIONS_REQUESTS_UNRESOLVED.getBuilder() );
    }

    @Override
    protected BoundStatement bind( AclRootPrincipalPair key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_ROOT.cql(), key.getAclRoot(), UUID.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getUser().getId() );
    }

    @Override
    protected BoundStatement bind( AclRootPrincipalPair key, PermissionsRequestDetails value, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_ROOT.cql(), key.getAclRoot(), UUID.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getUser().getId() )
                .setMap( CommonColumns.ACL_CHILDREN_PERMISSIONS.cql(),
                        value.getPermissions(),
                        TypeToken.of( UUID.class ),
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() )
                .set( CommonColumns.STATUS.cql(), value.getStatus(), RequestStatus.class );
    }

    @Override
    protected AclRootPrincipalPair mapKey( Row row ) {
        return new AclRootPrincipalPair( RowAdapters.aclRoot( row ), new Principal( PrincipalType.USER, RowAdapters.principalId( row ) ) );
    }

    @Override
    protected PermissionsRequestDetails mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return new PermissionsRequestDetails(
                RowAdapters.aclChildrenPermissions( row ),
                RowAdapters.reqStatus( row ) );
    }

    @Override
    public AclRootPrincipalPair generateTestKey() {
        return new AclRootPrincipalPair( TestDataFactory.aclKey(), TestDataFactory.userPrincipal() );
    }

    @Override
    public PermissionsRequestDetails generateTestValue() {
        return TestDataFactory.resolvedPRDetails();
    }
}

