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

import static com.openlattice.postgres.PostgresColumn.ACL_KEY;
import static com.openlattice.postgres.PostgresColumn.ANALYZER;
import static com.openlattice.postgres.PostgresColumn.DATATYPE;
import static com.openlattice.postgres.PostgresColumn.DESCRIPTION;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.NAME;
import static com.openlattice.postgres.PostgresColumn.NAMESPACE;
import static com.openlattice.postgres.PostgresColumn.PII;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE;
import static com.openlattice.postgres.PostgresColumn.SCHEMAS;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECT_TYPE;
import static com.openlattice.postgres.PostgresColumn.TITLE;

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

    public static PostgresTableDefinition NAMES =
            new PostgresTableDefinition( "names" )
                    .addColumns( ID, NAME );

    //    case NAMES:
    //            return new CassandraTableBuilder( NAMES )
    //                        .ifNotExists()
    //                        .partitionKey( SECURABLE_OBJECTID )
    //                        .columns( NAME );
    public static PostgresTableDefinition PROPERTY_TYPES =
            new PostgresTableDefinition( "property_types" )
                    .addColumns( ID, NAMESPACE, NAME, DATATYPE, TITLE, DESCRIPTION, SCHEMAS, PII, ANALYZER )
                    .setUnique( NAMESPACE, NAME );

    private PostgresTable() {
    }

}
