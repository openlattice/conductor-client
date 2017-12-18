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

package com.openlattice.edm;

import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.PROPERTY_ID;
import static com.openlattice.postgres.PostgresColumn.VERSION;
import static com.openlattice.postgres.PostgresDatatype.TIMESTAMPTZ;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresDatatype;
import com.openlattice.postgres.PostgresIndexDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.PostgresTableManager;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdmManager implements DbEdmManager {
    private static final Logger logger = LoggerFactory.getLogger( PostgresEdmManager.class );

    public static PostgresColumnDefinition LAST_WRITE   = new PostgresColumnDefinition( "last_write", TIMESTAMPTZ )
            .notNull();
    public static PostgresColumnDefinition LAST_INDEXED = new PostgresColumnDefinition( "last_indexed", TIMESTAMPTZ )
            .notNull();
    public static PostgresColumnDefinition READERS      = new PostgresColumnDefinition(
            "readers",
            PostgresDatatype.UUID );
    public static PostgresColumnDefinition WRITERS      = new PostgresColumnDefinition(
            "writers",
            PostgresDatatype.UUID );
    public static PostgresColumnDefinition OWNERS       = new PostgresColumnDefinition(
            "owners",
            PostgresDatatype.UUID );

    private final PostgresTableManager ptm;

    public PostgresEdmManager( PostgresTableManager ptm ) {
        this.ptm = ptm;
    }

    @Override
    public void createEntitySet(
            EntitySet entitySet,
            Set<PropertyType> propertyTypes ) throws SQLException {
        createEntitySetTable( entitySet );
        for ( PropertyType pt : propertyTypes ) {
            createPropertyTypeTableIfNotExist( entitySet, pt );
        }
        //Method is idempotent and should be re-executable in case of a failure.
    }

    @Override
    public void grant(
            Principal principal,
            EntitySet entitySet,
            Set<PropertyType> propertyTypes,
            EnumSet<Permission> permissions ) {
        if ( permissions.isEmpty() ) {
            //I hate early returns but nesting will get too messy and this is pretty clear that granting
            //no permissions is a no-op.
            return;
        }

        HikariDataSource hds = ptm.getHikariDataSource();

        List<String> tables = new ArrayList<>( propertyTypes.size() + 1 );
        tables.add( entityTableName( entitySet.getId() ) );

        for ( PropertyType pt : propertyTypes ) {
            tables.add( propertyTableName( entitySet.getId(), pt.getId() ) );
        }

        String principalId = principal.getId();

        for ( String table : tables ) {
            for ( Permission p : permissions ) {
                String postgresPrivilege = mapPermissionToPostgresPrivilege( p );
                String grantQuery = grantOnTable( table, principalId, postgresPrivilege );
                try ( Connection conn = hds.getConnection(); Statement s = conn.createStatement() ) {
                    s.execute( grantQuery );
                } catch ( SQLException e ) {
                    logger.error( "Unable to execute grant query {}", grantQuery, e );
                }
            }
        }
    }

    public void revoke(
            Principal principal,
            EntitySet entitySet,
            Set<PropertyType> propertyTypes,
            EnumSet<Permission> permissions ) {

    }

    @Override
    private String grantOnTable( String table, String principalId, String permission ) {
        return String.format( "GRANT %s ON TABLE %s TO %s", permission, table, principalId );
    }

    private void createEntitySetTable( EntitySet entitySet ) throws SQLException {
        PostgresTableDefinition ptd = buildEntitySetTableDefinition( entitySet );
        ptm.registerTables( ptd );
    }

    /*
     * Quick note on this function. It is IfNotExists only because PostgresTableDefinition queries
     * all include an if not exists. If the behavior of that class changes this function should be updated
     * appropriately.
     */
    private void createPropertyTypeTableIfNotExist( EntitySet entitySet, PropertyType propertyType )
            throws SQLException {
        PostgresTableDefinition ptd = buildPropertyTableDefinition( entitySet, propertyType );
        ptm.registerTables( ptd );
    }

    public static void changePermission(
            Principal principal,
            EntitySet entitySet,
            Set<PropertyType> propertyTypes,
            EnumSet<Permission> permissions ) {

    }

    public static void changePermission(
            Principal principal,
            EntitySet entitySet,
            Set<PropertyType> propertyTypes,
            EnumSet<Permission> permissions ) {

    }

    private static PostgresTableDefinition buildEntitySetTableDefinition( EntitySet entitySet ) {
        PostgresTableDefinition ptd = new PostgresTableDefinition( entityTableName( entitySet.getId() ) )
                .addColumns( ID, LAST_WRITE, LAST_INDEXED, READERS, WRITERS, OWNERS )
                .primaryKey( ID );

        PostgresIndexDefinition lastWriteIndex = new PostgresIndexDefinition( ptd, LAST_WRITE ).desc();
        PostgresIndexDefinition lastIndexedIndex = new PostgresIndexDefinition( ptd, LAST_INDEXED ).desc();

        PostgresIndexDefinition readersIndex = new PostgresIndexDefinition( ptd, READERS );
        PostgresIndexDefinition writersIndex = new PostgresIndexDefinition( ptd, WRITERS );
        PostgresIndexDefinition ownersIndex = new PostgresIndexDefinition( ptd, OWNERS );

        ptd.addIndexes( lastWriteIndex, lastIndexedIndex, readersIndex, writersIndex, ownersIndex );

        return ptd;
    }

    private static PostgresTableDefinition buildPropertyTableDefinition(
            EntitySet entitySet,
            PropertyType propertyType ) {
        PostgresColumnDefinition valueColumn = value( propertyType );
        PostgresTableDefinition ptd = new PostgresTableDefinition(
                propertyTableName( entitySet.getId(), propertyType.getId() ) )
                .addColumns( ID, PROPERTY_ID, valueColumn, VERSION, LAST_WRITE, LAST_INDEXED, READERS, WRITERS, OWNERS )
                .primaryKey( ID, PROPERTY_ID, valueColumn, VERSION );

        PostgresIndexDefinition valueIndex = new PostgresIndexDefinition( ptd, valueColumn );

        PostgresIndexDefinition readersIndex = new PostgresIndexDefinition( ptd, READERS );
        PostgresIndexDefinition writersIndex = new PostgresIndexDefinition( ptd, WRITERS );
        PostgresIndexDefinition ownersIndex = new PostgresIndexDefinition( ptd, OWNERS );

        ptd.addIndexes( valueIndex, readersIndex, writersIndex, ownersIndex );

        return ptd;
    }

    private static PostgresColumnDefinition value( PropertyType pt ) {
        return new PostgresColumnDefinition( "value", PostgresEdmTypeConverter.map( pt.getDatatype() ) );

    }

    private static String mapPermissionToPostgresPrivilege( Permission p ) {
        switch ( p ) {
            default:
                return p.name();
        }
    }

    public static String propertyTableName( UUID entitySetId, UUID propertyTypeId ) {
        return entitySetId.toString() + propertyTypeId.toString();
    }

    public static String entityTableName( UUID entitySetId ) {
        return entitySetId.toString();
    }
}
