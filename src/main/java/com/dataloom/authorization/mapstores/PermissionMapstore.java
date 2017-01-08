package com.dataloom.authorization.mapstores;

import java.util.EnumSet;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.codecs.EnumSetTypeCodec;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.mapstores.cassandra.AbstractStructuredCassandraMapstore;

import static com.dataloom.authorization.util.AuthorizationUtils.extractObjectType;

public class PermissionMapstore extends AbstractStructuredCassandraMapstore<AceKey, EnumSet<Permission>> {
    public static final String         MAP_NAME = "authorizations";
    public static final CassandraTableBuilder TABLE    = new CassandraTableBuilder(
            DatastoreConstants.KEYSPACE,
            PermissionMapstore.MAP_NAME )
                    .ifNotExists()
                    .partitionKey( CommonColumns.ACL_KEYS )
                    .clusteringColumns( CommonColumns.PRINCIPAL_TYPE, CommonColumns.PRINCIPAL_ID )
                    .columns( CommonColumns.SECURABLE_OBJECT_TYPE, CommonColumns.PERMISSIONS )
                    .secondaryIndex( CommonColumns.PERMISSIONS, CommonColumns.SECURABLE_OBJECT_TYPE );

    public PermissionMapstore( Session session ) {
        super( MAP_NAME, session, TABLE );
    }

    @Override
    protected BoundStatement bind( AceKey key, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), AclKeyPathFragment.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() );
    }

    @Override
    protected BoundStatement bind( AceKey key, EnumSet<Permission> permissions, BoundStatement bs ) {
        return bs.setList( CommonColumns.ACL_KEYS.cql(), key.getKey(), AclKeyPathFragment.class )
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), key.getPrincipal().getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), key.getPrincipal().getId() )
                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), extractObjectType( key ), SecurableObjectType.class )
                .set( CommonColumns.PERMISSIONS.cql(),
                        permissions,
                        EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    @Override
    protected AceKey mapKey( Row row ) {
        return AuthorizationUtils.getAceKeyFromRow( row );
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
}
