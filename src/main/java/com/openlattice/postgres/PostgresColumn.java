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

import static com.openlattice.postgres.PostgresDatatype.BOOLEAN;
import static com.openlattice.postgres.PostgresDatatype.TEXT;
import static com.openlattice.postgres.PostgresDatatype.TEXT_ARRAY;
import static com.openlattice.postgres.PostgresDatatype.UUID;
import static com.openlattice.postgres.PostgresDatatype.UUID_ARRAY;

import com.dataloom.edm.type.Analyzer;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class PostgresColumn {
    public static final String ACL_KEY_FIELD               = "acl_key";
    public static final String ANALYZER_FIELD              = "analyzer";
    public static final String DATATYPE_FIELD              = "datatype";
    public static final String DESCRIPTION_FIELD           = "description";
    public static final String ID_FIELD                    = "id";
    public static final String NAME_FIELD                  = "name";
    public static final String NAMESPACE_FIELD             = "namespace";
    public static final String TITLE_FIELD                 = "title";
    public static final String PII_FIELD                   = "pii";
    public static final String PERMISSIONS_FIELD            = "permissions";
    public static final String PRINCIPAL_TYPE_FIELD        = "principal_type";
    public static final String PRINCIPAL_ID_FIELD          = "principal_id";
    public static final String SCHEMAS_FIELD               = "schemas";
    public static final String SECURABLE_OBJECT_TYPE_FIELD = "securable_object_type";

    public static final PostgresColumnDefinition PERMISSIONS =
            new PostgresColumnDefinition( PERMISSIONS_FIELD, TEXT_ARRAY );
    public static final PostgresColumnDefinition PRINCIPAL_TYPE =
            new PostgresColumnDefinition( PRINCIPAL_TYPE_FIELD, TEXT );
    public static final PostgresColumnDefinition PRINCIPAL_ID =
            new PostgresColumnDefinition( PRINCIPAL_ID_FIELD, TEXT );
    public static PostgresColumnDefinition ACL_KEY  =
            new PostgresColumnDefinition( ACL_KEY_FIELD, UUID_ARRAY );
    public static PostgresColumnDefinition ANALYZER =
            new PostgresColumnDefinition( ANALYZER_FIELD, TEXT )
                    .withDefault( "'" + Analyzer.STANDARD.name() + "'" )
                    .notNull();
    public static PostgresColumnDefinition DATATYPE    =
            new PostgresColumnDefinition( DATATYPE_FIELD, TEXT ).notNull();
    public static PostgresColumnDefinition DESCRIPTION =
            new PostgresColumnDefinition( DESCRIPTION_FIELD, TEXT );
    public static PostgresColumnDefinition ID =
            new PostgresColumnDefinition( ID_FIELD, UUID ).primaryKey();
    public static PostgresColumnDefinition
            NAME =
            new PostgresColumnDefinition( NAME_FIELD, TEXT ).notNull();
    public static PostgresColumnDefinition NAMESPACE =
            new PostgresColumnDefinition( NAMESPACE_FIELD, TEXT ).notNull();
    public static PostgresColumnDefinition TITLE     =
            new PostgresColumnDefinition( TITLE_FIELD, TEXT ).notNull();
    public static PostgresColumnDefinition PII =
            new PostgresColumnDefinition( PII_FIELD, BOOLEAN )
                    .withDefault( false )
                    .notNull();
    public static PostgresColumnDefinition SECURABLE_OBJECT_TYPE =
            new PostgresColumnDefinition( SECURABLE_OBJECT_TYPE_FIELD, TEXT ).notNull();
    public static PostgresColumnDefinition SCHEMAS =
            new PostgresColumnDefinition( SCHEMAS_FIELD, TEXT_ARRAY )
                    .notNull();

    private PostgresColumn() {
    }

}
