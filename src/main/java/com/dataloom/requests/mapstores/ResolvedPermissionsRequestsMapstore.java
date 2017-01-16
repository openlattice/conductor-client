package com.dataloom.requests.mapstores;

import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.requests.AclRootRequestDetailsPair;
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

public class ResolvedPermissionsRequestsMapstore
        extends AbstractStructuredCassandraMapstore<PrincipalRequestIdPair, AclRootRequestDetailsPair> {
    public ResolvedPermissionsRequestsMapstore( Session session ) {
        super(
                HazelcastMap.PERMISSIONS_REQUESTS_RESOLVED.name(),
                session,
                Tables.PERMISSIONS_REQUESTS_RESOLVED.getBuilder() );
    }

    @Override
    protected BoundStatement bind( PrincipalRequestIdPair key, BoundStatement bs ) {
        return bs.setString( CommonColumns.PRINCIPAL_ID.cql(), key.getUser().getId() )
                .setUUID( CommonColumns.REQUESTID.cql(), key.getRequestId() );
    }

    @Override
    protected BoundStatement bind( PrincipalRequestIdPair key, AclRootRequestDetailsPair value, BoundStatement bs ) {
        return bs.setString( CommonColumns.PRINCIPAL_ID.cql(), key.getUser().getId() )
                .setUUID( CommonColumns.REQUESTID.cql(), key.getRequestId() )
                .setList( CommonColumns.ACL_ROOT.cql(), value.getAclRoot(), UUID.class )
                .setMap( CommonColumns.ACL_CHILDREN_PERMISSIONS.cql(),
                        value.getDetails().getPermissions(),
                        TypeToken.of( UUID.class ),
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() )
                .set( CommonColumns.STATUS.cql(), value.getDetails().getStatus(), RequestStatus.class );
    }

    @Override
    protected PrincipalRequestIdPair mapKey( Row row ) {
        return new PrincipalRequestIdPair(
                new Principal( PrincipalType.USER, RowAdapters.principalId( row ) ),
                RowAdapters.requestId( row ) );
    }

    @Override
    protected AclRootRequestDetailsPair mapValue( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return null;
        }
        return new AclRootRequestDetailsPair(
                RowAdapters.aclRoot( row ),
                new PermissionsRequestDetails(
                        RowAdapters.aclChildrenPermissions( row ),
                        RowAdapters.status( row ) ) );
    }

    @Override
    public PrincipalRequestIdPair generateTestKey() {
        throw new NotImplementedException(
                "GENERATION OF TEST KEY NOT IMPLEMENTED FOR RESOLVED PERMISSIONS REQUEST MAPSTORE." );
    }

    @Override
    public AclRootRequestDetailsPair generateTestValue() throws Exception {
        throw new NotImplementedException(
                "GENERATION OF TEST VALUE NOT IMPLEMENTED FOR RESOLVED PERMISSIONS REQUEST MAPSTORE." );
    }
}
