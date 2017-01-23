package com.dataloom.authorization.mapstores;

import java.util.EnumSet;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class PermissionMapstore extends AbstractStructuredCassandraMapstore<AceKey, DelegatedPermissionEnumSet> {
    public PermissionMapstore( Session session ) {
        super( HazelcastMap.PERMISSIONS.name(), session, Tables.PERMISSIONS.getBuilder() );
    }

    @Override
    protected BoundStatement bind( AceKey key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() );
    }

    @Override
    protected BoundStatement bind( AceKey key, DelegatedPermissionEnumSet permissions, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), UUID.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() )
                .set( CommonColumns.PERMISSIONS.cql(),
                        permissions.unwrap(),
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    @Override
    protected AceKey mapKey( Row row ) {
        return AuthorizationUtils.aceKey( row );
    }

    @Override
    protected DelegatedPermissionEnumSet mapValue( ResultSet rs ) {
        Row row = rs.one();
        return row == null ? null
                : DelegatedPermissionEnumSet.wrap( AuthorizationUtils.permissions( row ) );
    }

    @Override
    public AceKey generateTestKey() {
        return new AceKey(
                ImmutableList.of( UUID.randomUUID() ),
                new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ) );
    }

    @Override
    public DelegatedPermissionEnumSet generateTestValue() {
        return DelegatedPermissionEnumSet.wrap( EnumSet.of( Permission.READ, Permission.WRITE ) );
    }
}
