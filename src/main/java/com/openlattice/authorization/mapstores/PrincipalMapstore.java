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

package com.openlattice.authorization.mapstores;

import static com.openlattice.postgres.PostgresTable.PRINCIPALS;

import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.Role;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PrincipalMapstore extends AbstractBasePostgresMapstore<Principal, SecurablePrincipal> {
    private static Role TEST_ROLE = TestDataFactory.role();
    ;
    private static List<PostgresColumnDefinition> KEY_COLUMNS =
            ImmutableList.copyOf( PRINCIPALS.getPrimaryKey() );

    private static List<PostgresColumnDefinition> VALUE_COLUMNS =
            ImmutableList.copyOf( Sets.difference( PRINCIPALS.getColumns(), PRINCIPALS.getPrimaryKey() ) );

    public PrincipalMapstore( HikariDataSource hds ) {
        super( HazelcastMap.PRINCIPALS.name(), PRINCIPALS, hds );
    }

    @Override public Principal generateTestKey() {
        return TEST_ROLE.getPrincipal();
    }

    @Override public SecurablePrincipal generateTestValue() {
        return TEST_ROLE;
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return KEY_COLUMNS;
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return VALUE_COLUMNS;
    }

    @Override protected void bind(
            PreparedStatement ps, Principal key, SecurablePrincipal value ) throws SQLException {
        bind( ps, key );
        ps.setArray( 3, PostgresArrays.createUuidArray( ps.getConnection(), value.getAclKey().stream() ) );
        ps.setString( 4, value.getTitle() );
        ps.setString( 5, value.getDescription() );
    }

    @Override protected void bind( PreparedStatement ps, Principal key ) throws SQLException {
        ps.setString( 1, key.getType().name() );
        ps.setString( 2, key.getId() );
    }

    @Override
    protected SecurablePrincipal mapToValue( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.securablePrincipal( rs );
    }

    @Override protected Principal mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.principal( rs );
        } catch ( SQLException e ) {
            logger.error( "Unable to map row to principal", e );
            return null;
        }
    }
}
