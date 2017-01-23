package com.dataloom.requests.mapstores;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.Status;
import com.dataloom.requests.util.RequestUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

import java.util.UUID;

public class RequestMapstore extends AbstractStructuredCassandraMapstore<AceKey, Status> {
    private final Status TEST_STATUS = TestDataFactory.status();

    public RequestMapstore( Session session ) {
        super( HazelcastMap.REQUESTS.name(), session, Tables.REQUESTS.getBuilder() );
    }

    @Override
    protected BoundStatement bind( AceKey key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() );
    }

    @Override
    protected BoundStatement bind( AceKey key, Status status, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() )
                .set( CommonColumns.PERMISSIONS.cql(),
                        status.getPermissions(),
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() )
                .set( CommonColumns.STATUS.cql(), status.getStatus(), RequestStatus.class );
    }

    @Override
    protected AceKey mapKey( Row row ) {
        return AuthorizationUtils.aceKey( row );
    }

    @Override
    protected Status mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null
                : RequestUtil.status( row );
    }

    @Override
    public AceKey generateTestKey() {
        return RequestUtil.aceKey( TEST_STATUS );
    }

    @Override
    public Status generateTestValue() {
        return TEST_STATUS;
    }
}
