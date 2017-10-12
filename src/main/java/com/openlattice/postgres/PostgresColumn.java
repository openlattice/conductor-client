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

import com.dataloom.edm.type.Analyzer;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class PostgresColumn {
    public static PostgresColumnDefinition ID          =
            new PostgresColumnDefinition( "id", UUID )
                    .primaryKey();
    public static PostgresColumnDefinition NAME        =
            new PostgresColumnDefinition( "name", TEXT ).notNull();
    public static PostgresColumnDefinition NAMESPACE   =
            new PostgresColumnDefinition( "namespace", TEXT ).notNull();
    public static PostgresColumnDefinition DATATYPE       =
            new PostgresColumnDefinition( "datatype", TEXT ).notNull();
    public static PostgresColumnDefinition TITLE       =
            new PostgresColumnDefinition( "title", TEXT ).notNull();
    public static PostgresColumnDefinition DESCRIPTION =
            new PostgresColumnDefinition( "description", TEXT );
    public static PostgresColumnDefinition PII         =
            new PostgresColumnDefinition( "pii", BOOLEAN )
                    .withDefault( false )
                    .notNull();
    public static PostgresColumnDefinition ANALYZER    =
            new PostgresColumnDefinition( "analyzer", TEXT )
                    .withDefault( "'" + Analyzer.STANDARD.name() + "'" )
                    .notNull();
    public static PostgresColumnDefinition SCHEMAS     =
            new PostgresColumnDefinition( "schemas", TEXT_ARRAY )
                    .notNull();

}
