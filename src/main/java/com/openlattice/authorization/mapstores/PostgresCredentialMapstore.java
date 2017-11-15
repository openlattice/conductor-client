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

import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableList;
import com.openlattice.authorization.DbCredentialQueryService;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.RandomStringUtils;

/**
 * This mapstore assumes that initial first time creation of user in postgres is handled externally.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresCredentialMapstore extends AbstractBasePostgresMapstore<String, String> {
    private final DbCredentialQueryService dcqs;

    public PostgresCredentialMapstore( HikariDataSource hds ) {
        super( HazelcastMap.DB_CREDS.name(), PostgresTable.DB_CREDS, hds );
        this.dcqs = new DbCredentialQueryService( hds );
    }

    @Override public String generateTestKey() {
        return RandomStringUtils.random( 5 );
    }

    @Override public String generateTestValue() {
        return RandomStringUtils.random( 5 );
    }

    @Override protected List<PostgresColumnDefinition> keyColumns() {
        return ImmutableList.of( PostgresColumn.PRINCIPAL_ID );
    }

    @Override protected List<PostgresColumnDefinition> valueColumns() {
        return ImmutableList.of( PostgresColumn.CREDENTIAL );
    }

    @Override protected void bind( PreparedStatement ps, String key, String value ) throws SQLException {
        bind( ps, key );
        ps.setString( 2, value );
    }

    @Override protected void bind( PreparedStatement ps, String key ) throws SQLException {
        ps.setString( 1, key );
    }

    @Override public void store( String key, String value ) {
        super.store( key, value );
        dcqs.setCredential( key, value );
    }

    @Override public void storeAll( Map<String, String> map ) {
        super.storeAll( map );
        map.entrySet().forEach( p -> dcqs.setCredential( p.getKey(), p.getValue() ) );
    }

    @Override public void delete( String key ) {
        super.delete( key );
        dcqs.deleteCredential( key );
    }

    @Override public void deleteAll( Collection<String> keys ) {
        super.deleteAll( keys );
        keys.forEach( u -> dcqs.deleteCredential( u ) );
    }

    @Override protected String mapToValue( ResultSet rs ) throws SQLException {
        return rs.getString( PostgresColumn.CREDENTIAL_FIELD );
    }

    @Override protected String mapToKey( ResultSet rs ) throws SQLException {
        return rs.getString( PostgresColumn.PRINCIPAL_ID_FIELD );
    }
}
