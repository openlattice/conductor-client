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

package com.openlattice.postgres;

import static com.openlattice.postgres.PostgresColumn.*;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class PostgresTable {
    public static PostgresTableDefinition SECURABLE_OBJECTS =
            new PostgresTableDefinition( "securable_objects" )
                    .addColumns( ACL_KEY, SECURABLE_OBJECT_TYPE );

    public static PostgresTableDefinition PERMISSIONS =
            new PostgresTableDefinition( "permissions" )
                    .addColumns( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, PostgresColumn.PERMISSIONS )
                    .primaryKey( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );

    //                case PERMISSIONS:
    // TODO: Once Cassandra fixes SASI + Collection column inde
    //            return new CassandraTableBuilder( PERMISSIONS )
    //                        .ifNotExists()
    //                        .partitionKey( CommonColumns.ACL_KEYS )
    //                        .clusteringColumns( PRINCIPAL_TYPE, PRINCIPAL_ID )
    //                        .columns( CommonColumns.PERMISSIONS )
    //                        .staticColumns( SECURABLE_OBJECT_TYPE )
    //                        .secondaryIndex( PRINCIPAL_TYPE,
    //            PRINCIPAL_ID,
    //            CommonColumns.PERMISSIONS,
    //            SECURABLE_OBJECT_TYPE );

    public static PostgresTableDefinition PROPERTY_TYPES =
            new PostgresTableDefinition( "property_types" )
                    .addColumns( ID, NAMESPACE, NAME, DATATYPE, TITLE, DESCRIPTION, SCHEMAS, PII, ANALYZER )
                    .setUnique( NAMESPACE, NAME );

    public static PostgresTableDefinition ACL_KEYS =
            new PostgresTableDefinition( "acl_keys" )
                    .addColumns( NAME, SECURABLE_OBJECTID )
                    .primaryKey( NAME );

    public static PostgresTableDefinition NAMES =
            new PostgresTableDefinition( "names" )
                    .addColumns( SECURABLE_OBJECTID, NAME )
                    .primaryKey( SECURABLE_OBJECTID );

    public static PostgresTableDefinition ENTITY_TYPES =
            new PostgresTableDefinition( "entity_types" )
                    .addColumns( ID, NAMESPACE, NAME, TITLE, DESCRIPTION, KEY, PROPERTIES, BASE_TYPE, SCHEMAS, CATEGORY )
                    .setUnique( NAMESPACE, NAME );

    public static PostgresTableDefinition ENTITY_SETS =
            new PostgresTableDefinition( "entity_sets" )
                    .addColumns( ID, NAME, ENTITY_TYPE_ID, TITLE, DESCRIPTION, CONTACTS )
                    .setUnique( NAME );

    public static PostgresTableDefinition LINKING_VERTICES =
            new PostgresTableDefinition( "linking_vertices" )
                    .addColumns( GRAPH_ID, VERTEX_ID, GRAPH_DIAMETER, ENTITY_KEY_IDS )
                    .primaryKey( GRAPH_ID, VERTEX_ID );

    public static PostgresTableDefinition ASSOCIATION_TYPES =
            new PostgresTableDefinition( "association_types" )
                    .addColumns( ID, SRC, DST, BIDIRECTIONAL );

    private PostgresTable() {
    }

}
