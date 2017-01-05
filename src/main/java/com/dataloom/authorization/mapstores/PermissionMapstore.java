package com.dataloom.authorization.mapstores;

import java.util.EnumSet;
import java.util.List;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.authorization.util.CassandraMappingUtils;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

public class PermissionMapstore extends AbstractStructuredCassandraMapstore<AceKey, EnumSet<Permission>> {
    public static final String MAP_NAME = "authorizations";

    public PermissionMapstore(
            Session session,
            CassandraTableBuilder tableBuilder ) {
        super( MAP_NAME, session, tableBuilder );
    }

    @Override
    protected BoundStatement bind( AceKey key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), AclKey.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() );
    }

    @Override
    protected BoundStatement bind( AceKey key, EnumSet<Permission> permissions, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), AclKey.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() )
                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), extractObjectType( key ), SecurableObjectType.class )
                .set( CommonColumns.PERMISSIONS.cql(),
                        permissions,
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    @Override
    protected AceKey mapKey( Row row ) {
        return CassandraMappingUtils.getAceKeyFromRow( row );
    }

    @Override
    protected EnumSet<Permission> mapValue( Row row ) {
        return row.get( CommonColumns.PERMISSIONS.cql(), EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    @Override
    public AceKey generateTestKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EnumSet<Permission> generateTestValue() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    private static SecurableObjectType extractObjectType( AceKey key ) {
        final List<AclKey> aclKeys = key.getKey();
        final int aclKeyCount = aclKeys.size();
        return aclKeyCount > 0 ? aclKeys.get( aclKeyCount - 1 ).getType() : null;
    }
}
