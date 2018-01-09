/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.postgres.mapstores.data;

import com.openlattice.data.PropertyKey;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresDatatype;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiFunction;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PropertyDataMapstore extends AbstractBasePostgresMapstore<PropertyKey, PropertyMetadata> {

    public PropertyDataMapstore(
            PostgresTableDefinition table,
            HikariDataSource hds
    ) {
        //Table name doesn't matter as these aer used for configuring maps.
        super( "pdms", table, hds );
    }

    @Override public PropertyKey generateTestKey() {
        return null;
    }

    @Override public PropertyMetadata generateTestValue() {
        return null;
    }

    @Override protected void bind( PreparedStatement ps, PropertyKey key, PropertyMetadata value ) throws SQLException {
        int parameterIndex = bind( ps, key, 1);
        ps.setLong( parameterIndex++, value.getVersion() );
        ps.setArray( parameterIndex++, PostgresArrays.createLongArray( ps.getConnection(),value.getVersions() ) );
        ps.setObject( parameterIndex++, value.getLastWrite() );
    }

    @Override protected int bind( PreparedStatement ps, PropertyKey key, int offset ) throws SQLException {
        ps.setObject( offset++, key.getEntityKeyId() );
        ps.setObject( offset++, key.getValue() );
        return offset;
    }

    @Override protected PropertyMetadata mapToValue( ResultSet rs ) throws SQLException {
        return null;
    }

    @Override protected PropertyKey mapToKey( ResultSet rs ) throws SQLException {
        return null;
    }

    protected void bind( PreparedStatement ps, long key ) throws SQLException {
        ps.setLong( 1, key );
    }
}
