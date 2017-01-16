package com.dataloom.requests.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
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
                RowAdapters.status( row ) );
    }

    @Override
    public AclRootPrincipalPair generateTestKey() {
        throw new NotImplementedException(
                "GENERATION OF TEST KEY NOT IMPLEMENTED FOR UNRESOLVED PERMISSIONS REQUEST MAPSTORE." );
    }

    @Override
    public PermissionsRequestDetails generateTestValue() throws Exception {
        throw new NotImplementedException(
                "GENERATION OF TEST VALUE NOT IMPLEMENTED FOR UNRESOLVED PERMISSIONS REQUEST MAPSTORE." );
    }
}

