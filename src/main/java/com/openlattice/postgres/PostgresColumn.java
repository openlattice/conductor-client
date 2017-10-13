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

import com.dataloom.edm.type.Analyzer;

import static com.openlattice.postgres.PostgresDatatype.*;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class PostgresColumn {
    public static final String ACL_KEY_FIELD               = "acl_key";
    public static final String ALLOWED_EMAIL_DOMAINS_FIELD = "allowed_email_domains";
    public static final String ANALYZER_FIELD              = "analyzer";
    public static final String BASE_TYPE_FIELD             = "base_type";
    public static final String BIDIRECTIONAL_FIELD         = "bidirectional";
    public static final String CATEGORY_FIELD              = "category";
    public static final String CONTACTS_FIELD              = "contacts";
    public static final String DATATYPE_FIELD              = "datatype";
    public static final String DESCRIPTION_FIELD           = "description";
    public static final String DST_FIELD                   = "dst";
    public static final String ENTITY_KEY_IDS_FIELD        = "entity_key_ids";
    public static final String ENTITY_TYPE_ID_FIELD        = "entity_type_id";
    public static final String GRAPH_DIAMETER_FIELD        = "graph_diameter";
    public static final String GRAPH_ID_FIELD              = "graph_id";
    public static final String ID_FIELD                    = "id";
    public static final String KEY_FIELD                   = "key";
    public static final String MEMBERS_FIELD               = "members";
    public static final String NAME_FIELD                  = "name";
    public static final String NAMESPACE_FIELD             = "namespace";
    public static final String ORGANIZATION_TITLE_FIELD    = "title";
    public static final String TITLE_FIELD                 = "title";
    public static final String PII_FIELD                   = "pii";
    public static final String PERMISSIONS_FIELD           = "permissions";
    public static final String PRINCIPAL_TYPE_FIELD        = "principal_type";
    public static final String PRINCIPAL_ID_FIELD          = "principal_id";
    public static final String PROPERTIES_FIELD            = "properties";
    public static final String SCHEMAS_FIELD               = "schemas";
    public static final String SECURABLE_OBJECT_TYPE_FIELD = "securable_object_type";
    public static final String SECURABLE_OBJECTID_FIELD    = "securable_objectid";
    public static final String SRC_FIELD                   = "src";
    public static final String VERTEX_ID_FIELD             = "vertex_id";

    public static final PostgresColumnDefinition PERMISSIONS           =
            new PostgresColumnDefinition( PERMISSIONS_FIELD, TEXT_ARRAY );
    public static final PostgresColumnDefinition PRINCIPAL_TYPE        =
            new PostgresColumnDefinition( PRINCIPAL_TYPE_FIELD, TEXT );
    public static final PostgresColumnDefinition PRINCIPAL_ID          =
            new PostgresColumnDefinition( PRINCIPAL_ID_FIELD, TEXT );
    public static       PostgresColumnDefinition ACL_KEY               =
            new PostgresColumnDefinition( ACL_KEY_FIELD, UUID_ARRAY );
    public static       PostgresColumnDefinition ANALYZER              =
            new PostgresColumnDefinition( ANALYZER_FIELD, TEXT )
                    .withDefault( "'" + Analyzer.STANDARD.name() + "'" )
                    .notNull();
    public static       PostgresColumnDefinition DATATYPE              =
            new PostgresColumnDefinition( DATATYPE_FIELD, TEXT ).notNull();
    public static       PostgresColumnDefinition DESCRIPTION           =
            new PostgresColumnDefinition( DESCRIPTION_FIELD, TEXT );
    public static       PostgresColumnDefinition ID                    =
            new PostgresColumnDefinition( ID_FIELD, UUID ).primaryKey();
    public static       PostgresColumnDefinition
                                                 NAME                  =
            new PostgresColumnDefinition( NAME_FIELD, TEXT ).notNull();
    public static       PostgresColumnDefinition NAMESPACE             =
            new PostgresColumnDefinition( NAMESPACE_FIELD, TEXT ).notNull();
    public static       PostgresColumnDefinition TITLE                 =
            new PostgresColumnDefinition( TITLE_FIELD, TEXT ).notNull();
    public static       PostgresColumnDefinition PII                   =
            new PostgresColumnDefinition( PII_FIELD, BOOLEAN )
                    .withDefault( false )
                    .notNull();
    public static       PostgresColumnDefinition SECURABLE_OBJECT_TYPE =
            new PostgresColumnDefinition( SECURABLE_OBJECT_TYPE_FIELD, TEXT ).notNull();
    public static       PostgresColumnDefinition SCHEMAS               =
            new PostgresColumnDefinition( SCHEMAS_FIELD, TEXT_ARRAY )
                    .notNull();
    public static       PostgresColumnDefinition SECURABLE_OBJECTID    =
            new PostgresColumnDefinition( SECURABLE_OBJECTID_FIELD, UUID ).notNull();

    public static PostgresColumnDefinition KEY            =
            new PostgresColumnDefinition( KEY_FIELD, UUID_ARRAY ).notNull();
    public static PostgresColumnDefinition PROPERTIES     =
            new PostgresColumnDefinition( PROPERTIES_FIELD, UUID_ARRAY ).notNull();
    public static PostgresColumnDefinition CATEGORY       =
            new PostgresColumnDefinition( CATEGORY_FIELD, TEXT ).notNull();
    public static PostgresColumnDefinition BASE_TYPE      =
            new PostgresColumnDefinition( BASE_TYPE_FIELD, UUID );
    public static PostgresColumnDefinition ENTITY_TYPE_ID =
            new PostgresColumnDefinition( ENTITY_TYPE_ID_FIELD, UUID ).notNull();
    public static PostgresColumnDefinition CONTACTS       =
            new PostgresColumnDefinition( CONTACTS_FIELD, TEXT_ARRAY );
    public static PostgresColumnDefinition GRAPH_DIAMETER =
            new PostgresColumnDefinition( GRAPH_DIAMETER_FIELD, DECIMAL );
    public static PostgresColumnDefinition ENTITY_KEY_IDS =
            new PostgresColumnDefinition( ENTITY_KEY_IDS_FIELD, UUID_ARRAY );
    public static PostgresColumnDefinition GRAPH_ID       =
            new PostgresColumnDefinition( GRAPH_ID_FIELD, UUID );
    public static PostgresColumnDefinition VERTEX_ID      =
            new PostgresColumnDefinition( VERTEX_ID_FIELD, UUID );
    public static PostgresColumnDefinition SRC =
            new PostgresColumnDefinition( SRC_FIELD, UUID_ARRAY );
    public static PostgresColumnDefinition DST =
            new PostgresColumnDefinition( DST_FIELD, UUID_ARRAY );
    public static PostgresColumnDefinition BIDIRECTIONAL =
            new PostgresColumnDefinition( BIDIRECTIONAL_FIELD, BOOLEAN );
    public static PostgresColumnDefinition ALLOWED_EMAIL_DOMAINS =
            new PostgresColumnDefinition( ALLOWED_EMAIL_DOMAINS_FIELD, TEXT_ARRAY );
    public static PostgresColumnDefinition MEMBERS =
            new PostgresColumnDefinition( MEMBERS_FIELD, TEXT_ARRAY );
    public static PostgresColumnDefinition ORGANIZATION_TITLE =
            new PostgresColumnDefinition( ORGANIZATION_TITLE_FIELD, TEXT );

    private PostgresColumn() {
    }

}
