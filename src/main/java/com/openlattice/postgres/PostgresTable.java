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
import static com.openlattice.postgres.PostgresColumn.ACL_KEY;
import static com.openlattice.postgres.PostgresColumn.ALLOWED_EMAIL_DOMAINS;
import static com.openlattice.postgres.PostgresColumn.ANALYZER;
import static com.openlattice.postgres.PostgresColumn.AUDIT_ID;
import static com.openlattice.postgres.PostgresColumn.BASE_TYPE;
import static com.openlattice.postgres.PostgresColumn.BIDIRECTIONAL;
import static com.openlattice.postgres.PostgresColumn.BLOCK_ID;
import static com.openlattice.postgres.PostgresColumn.CATEGORY;
import static com.openlattice.postgres.PostgresColumn.CONTACTS;
import static com.openlattice.postgres.PostgresColumn.CURRENT_SYNC_ID;
import static com.openlattice.postgres.PostgresColumn.DATATYPE;
import static com.openlattice.postgres.PostgresColumn.DATA_ID;
import static com.openlattice.postgres.PostgresColumn.DESCRIPTION;
import static com.openlattice.postgres.PostgresColumn.DST;
import static com.openlattice.postgres.PostgresColumn.DST_LINKING_VERTEX_ID;
import static com.openlattice.postgres.PostgresColumn.EDGE_VALUE;
import static com.openlattice.postgres.PostgresColumn.EDM_VERSION;
import static com.openlattice.postgres.PostgresColumn.EDM_VERSION_NAME;
import static com.openlattice.postgres.PostgresColumn.ENTITY_KEY_IDS;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_IDS;
import static com.openlattice.postgres.PostgresColumn.ENTITY_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.EVENT_TYPE;
import static com.openlattice.postgres.PostgresColumn.FLAGS;
import static com.openlattice.postgres.PostgresColumn.GRAPH_DIAMETER;
import static com.openlattice.postgres.PostgresColumn.GRAPH_ID;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.ID_VALUE;
import static com.openlattice.postgres.PostgresColumn.KEY;
import static com.openlattice.postgres.PostgresColumn.MEMBERS;
import static com.openlattice.postgres.PostgresColumn.NAME;
import static com.openlattice.postgres.PostgresColumn.NAMESPACE;
import static com.openlattice.postgres.PostgresColumn.NAME_SET;
import static com.openlattice.postgres.PostgresColumn.NEW_VERTEX_ID;
import static com.openlattice.postgres.PostgresColumn.NULLABLE_TITLE;
import static com.openlattice.postgres.PostgresColumn.ORGANIZATION_ID;
import static com.openlattice.postgres.PostgresColumn.PII;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_IDS;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE;
import static com.openlattice.postgres.PostgresColumn.PROPERTIES;
import static com.openlattice.postgres.PostgresColumn.PROPERTY_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.REASON;
import static com.openlattice.postgres.PostgresColumn.ROLE_ID;
import static com.openlattice.postgres.PostgresColumn.SCHEMAS;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECTID;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECT_TYPE;
import static com.openlattice.postgres.PostgresColumn.SHOW;
import static com.openlattice.postgres.PostgresColumn.SRC;
import static com.openlattice.postgres.PostgresColumn.SRC_LINKING_VERTEX_ID;
import static com.openlattice.postgres.PostgresColumn.STATUS;
import static com.openlattice.postgres.PostgresColumn.SYNC_ID;
import static com.openlattice.postgres.PostgresColumn.TIME_UUID;
import static com.openlattice.postgres.PostgresColumn.TITLE;
import static com.openlattice.postgres.PostgresColumn.VERTEX_ID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class PostgresTable {

    public static final PostgresTableDefinition ACL_KEYS =
            new PostgresTableDefinition( "acl_keys" )
                    .addColumns( NAME, SECURABLE_OBJECTID )
                    .primaryKey( NAME )
                    .setUnique( NAME );

    public static final PostgresTableDefinition APP_CONFIGS =
            new PostgresTableDefinition( "app_configs" )
                    .addColumns( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID, PostgresColumn.PERMISSIONS, ENTITY_SET_ID )
                    .primaryKey( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID );

    public static final PostgresTableDefinition APP_TYPES =
            new PostgresTableDefinition( "app_types" )
                    .addColumns( ID, NAMESPACE, NAME, TITLE, DESCRIPTION, ENTITY_TYPE_ID )
                    .setUnique( NAMESPACE, NAME );

    public static final PostgresTableDefinition APPS =
            new PostgresTableDefinition( "apps" )
                    .addColumns( ID, NAME, TITLE, DESCRIPTION, CONFIG_TYPE_IDS )
                    .setUnique( NAME );

    public static final PostgresTableDefinition ASSOCIATION_TYPES =
            new PostgresTableDefinition( "association_types" )
                    .addColumns( ID, SRC, DST, BIDIRECTIONAL );

    public static final PostgresTableDefinition AUDIT_LOG =
            new PostgresTableDefinition( "audit_log" )
                    .addColumns( ACL_KEY,
                            EVENT_TYPE,
                            PRINCIPAL_TYPE,
                            PRINCIPAL_ID,
                            TIME_UUID,
                            AUDIT_ID,
                            DATA_ID,
                            BLOCK_ID )
                    .primaryKey( ACL_KEY, EVENT_TYPE, PRINCIPAL_TYPE, PRINCIPAL_ID, TIME_UUID )
                    .setUnique( ACL_KEY, EVENT_TYPE, PRINCIPAL_TYPE, PRINCIPAL_ID, TIME_UUID );

    public static final PostgresTableDefinition COMPLEX_TYPES =
            new PostgresTableDefinition( "complex_types" )
                    .addColumns( ID, NAMESPACE, NAME, TITLE, DESCRIPTION, PROPERTIES, BASE_TYPE, SCHEMAS, CATEGORY )
                    .setUnique( NAMESPACE, NAME );

    public static final PostgresTableDefinition EDM_VERSIONS =
            new PostgresTableDefinition( "edm_versions" )
                    .addColumns( EDM_VERSION_NAME, EDM_VERSION )
                    .primaryKey( EDM_VERSION_NAME, EDM_VERSION )
                    .setUnique( EDM_VERSION_NAME, EDM_VERSION );

    public static final PostgresTableDefinition ENTITY_SET_PROPERTY_METADATA =
            new PostgresTableDefinition( "entity_set_property_metadata" )
                    .addColumns( ENTITY_SET_ID, PROPERTY_TYPE_ID, TITLE, DESCRIPTION, SHOW )
                    .primaryKey( ENTITY_SET_ID, PROPERTY_TYPE_ID );

    public static final PostgresTableDefinition ENTITY_SETS =
            new PostgresTableDefinition( "entity_sets" )
                    .addColumns( ID, NAME, ENTITY_TYPE_ID, TITLE, DESCRIPTION, CONTACTS )
                    .setUnique( NAME );

    public static final PostgresTableDefinition ENTITY_TYPES =
            new PostgresTableDefinition( "entity_types" )
                    .addColumns( ID,
                            NAMESPACE,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            KEY,
                            PROPERTIES,
                            BASE_TYPE,
                            SCHEMAS,
                            CATEGORY )
                    .setUnique( NAMESPACE, NAME );

    public static final PostgresTableDefinition ENUM_TYPES =
            new PostgresTableDefinition( "enum_types" )
                    .addColumns( ID,
                            NAMESPACE,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            MEMBERS,
                            SCHEMAS,
                            DATATYPE,
                            FLAGS,
                            PII,
                            ANALYZER )
                    .setUnique( NAMESPACE, NAME );

    public static final PostgresTableDefinition LINKED_ENTITY_SETS =
            new PostgresTableDefinition( "linked_entity_sets" )
                    .addColumns( ID, ENTITY_SET_IDS );

    public static final PostgresTableDefinition LINKING_EDGES =
            new PostgresTableDefinition( "linking_edges" )
                    .addColumns( GRAPH_ID, SRC_LINKING_VERTEX_ID, EDGE_VALUE, DST_LINKING_VERTEX_ID )
                    .primaryKey( GRAPH_ID, SRC_LINKING_VERTEX_ID, EDGE_VALUE, DST_LINKING_VERTEX_ID );

    public static final PostgresTableDefinition LINKING_VERTICES =
            new PostgresTableDefinition( "linking_vertices" )
                    .addColumns( GRAPH_ID, VERTEX_ID, GRAPH_DIAMETER, ENTITY_KEY_IDS )
                    .primaryKey( GRAPH_ID, VERTEX_ID );

    public static final PostgresTableDefinition NAMES =
            new PostgresTableDefinition( "names" )
                    .addColumns( SECURABLE_OBJECTID, NAME )
                    .primaryKey( SECURABLE_OBJECTID );

    public static final PostgresTableDefinition ORGANIZATIONS =
            new PostgresTableDefinition( "organizations" )
                    .addColumns( ID, NULLABLE_TITLE, DESCRIPTION, ALLOWED_EMAIL_DOMAINS, MEMBERS );

    public static final PostgresTableDefinition PERMISSIONS =
            new PostgresTableDefinition( "permissions" )
                    .addColumns( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, PostgresColumn.PERMISSIONS )
                    .primaryKey( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );

    public static final PostgresTableDefinition PROPERTY_TYPES =
            new PostgresTableDefinition( "property_types" )
                    .addColumns( ID, NAMESPACE, NAME, DATATYPE, TITLE, DESCRIPTION, SCHEMAS, PII, ANALYZER )
                    .setUnique( NAMESPACE, NAME );

    public static final PostgresTableDefinition REQUESTS =
            new PostgresTableDefinition( "requests" )
                    .addColumns( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, PostgresColumn.PERMISSIONS, REASON, STATUS )
                    .primaryKey( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );

    public static final PostgresTableDefinition ROLES =
            new PostgresTableDefinition( "roles" )
                    .addColumns( ROLE_ID, ORGANIZATION_ID, NULLABLE_TITLE, DESCRIPTION, PRINCIPAL_IDS )
                    .primaryKey( ROLE_ID, ORGANIZATION_ID );

    public static final PostgresTableDefinition SCHEMA =
            new PostgresTableDefinition( "schemas" )
                    .addColumns( NAMESPACE, NAME_SET )
                    .primaryKey( NAMESPACE );

    public static final PostgresTableDefinition SECURABLE_OBJECTS =
            new PostgresTableDefinition( "securable_objects" )
                    .addColumns( ACL_KEY, SECURABLE_OBJECT_TYPE )
                    .primaryKey( ACL_KEY );

    public static final PostgresTableDefinition SYNC_IDS =
            new PostgresTableDefinition( "sync_ids" )
                    .addColumns( ENTITY_SET_ID, SYNC_ID, CURRENT_SYNC_ID )
                    .primaryKey( ENTITY_SET_ID, SYNC_ID )
                    .setUnique( ENTITY_SET_ID, SYNC_ID );

    public static final PostgresTableDefinition VERTEX_IDS_AFTER_LINKING =
            new PostgresTableDefinition( "vertex_ids_after_linking" )
                    .addColumns( GRAPH_ID, VERTEX_ID, NEW_VERTEX_ID )
                    .primaryKey( GRAPH_ID, VERTEX_ID );

    public static final PostgresTableDefinition PRINCIPALS =
            new PostgresTableDefinition( "principals" )
                    .addColumns( PRINCIPAL_TYPE, PRINCIPAL_ID, ACL_KEY, NULLABLE_TITLE, DESCRIPTION )
                    .primaryKey( PRINCIPAL_TYPE, PRINCIPAL_ID )
                    .setUnique( ID_VALUE );
    // Consider enforcing ACL_KEY a unique field

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

    private PostgresTable() {
    }

}
