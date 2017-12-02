package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.ACL_KEY;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECT_TYPE;
import static com.openlattice.postgres.PostgresTable.SECURABLE_OBJECTS;

import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.authorization.AclKey;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public class SecurableObjectTypeMapstore extends AbstractBasePostgresMapstore<AclKey, SecurableObjectType> {

    public SecurableObjectTypeMapstore( HikariDataSource hds ) {
        super( HazelcastMap.SECURABLE_OBJECT_TYPES.name(), SECURABLE_OBJECTS, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ACL_KEY );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( SECURABLE_OBJECT_TYPE );
    }

    @Override protected void bind(
            PreparedStatement ps, AclKey key, SecurableObjectType value ) throws SQLException {
        bind( ps, key );
        ps.setString( 2, value.name() );
        ps.setString( 3, value.name() );
    }

    @Override protected void bind( PreparedStatement ps, AclKey key ) throws SQLException {
        ps.setArray( 1, PostgresArrays.createUuidArray( ps.getConnection(), key ) );
    }

    @Override protected SecurableObjectType mapToValue( ResultSet rs ) throws SQLException {
        SecurableObjectType objectType = ResultSetAdapters.securableObjectType( rs );
        return objectType == null ? SecurableObjectType.Unknown : objectType;
    }

    @Override protected AclKey mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.aclKey( rs );
    }

    @Override public AclKey generateTestKey() {
        return new AclKey( UUID.randomUUID(), UUID.randomUUID() );
    }

    @Override public SecurableObjectType generateTestValue() {
        return SecurableObjectType.EntitySet;
    }
}
