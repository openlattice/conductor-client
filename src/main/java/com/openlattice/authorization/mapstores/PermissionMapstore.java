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

import static com.openlattice.postgres.PostgresArrays.createTextArray;
import static com.openlattice.postgres.PostgresArrays.createUuidArray;
import static com.openlattice.postgres.PostgresColumn.ACL_KEY;
import static com.openlattice.postgres.PostgresColumn.PERMISSIONS;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PermissionMapstore extends AbstractBasePostgresMapstore<AceKey, DelegatedPermissionEnumSet> {

    public PermissionMapstore(
            String mapName,
            PostgresTableDefinition table,
            HikariDataSource hds ) {
        super( mapName, table, hds );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( PERMISSIONS );
    }

    @Override protected void bind(
            PreparedStatement ps, AceKey key, DelegatedPermissionEnumSet value ) throws SQLException {
        bind( ps, key );
        Array permissions = createTextArray( ps.getConnection(), value.stream().map( Permission::name ) );
        ps.setArray( 4, permissions );
        ps.setArray( 5, permissions );
    }

    @Override protected void bind( PreparedStatement ps, AceKey key ) throws SQLException {
        Principal p = key.getPrincipal();
        ps.setArray( 1, createUuidArray( ps.getConnection(), key.getKey().stream() ) );
        ps.setString( 2, p.getType().name() );
        ps.setString( 3, p.getId() );
    }

    @Override protected DelegatedPermissionEnumSet mapToValue( ResultSet rs ) throws SQLException {
        EnumSet<Permission> pset = ResultSetAdapters.permissions( rs );
        return DelegatedPermissionEnumSet.wrap( pset );
    }

    @Override protected AceKey mapToKey( ResultSet rs ) {
        try {
            return ResultSetAdapters.aceKey( rs );
        } catch ( SQLException ex ) {
            logger.error( "Unable to map ace key from result set in permissions mapstore", ex );
            return null;
        }
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
