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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
public class PostgresTablesPod {
    @Bean
    public PostgresTables postgresTables() {
        return () -> Stream.of(
                PostgresTable.ACL_KEYS,
                PostgresTable.APP_CONFIGS,
                PostgresTable.APP_TYPES,
                PostgresTable.APPS,
                PostgresTable.ASSOCIATION_TYPES,
                PostgresTable.AUDIT_LOG,
                PostgresTable.COMPLEX_TYPES,
                PostgresTable.EDM_VERSIONS,
                PostgresTable.ENTITY_SET_PROPERTY_METADATA,
                PostgresTable.ENTITY_SETS,
                PostgresTable.ENTITY_TYPES,
                PostgresTable.ENUM_TYPES,
                PostgresTable.LINKED_ENTITY_SETS,
                PostgresTable.LINKING_VERTICES,
                PostgresTable.NAMES,
                PostgresTable.ORGANIZATIONS,
                PostgresTable.PERMISSIONS,
                PostgresTable.PRINCIPALS,
                PostgresTable.PROPERTY_TYPES,
                PostgresTable.REQUESTS,
                PostgresTable.ROLES,
                PostgresTable.SCHEMA,
                PostgresTable.SECURABLE_OBJECTS,
                PostgresTable.SYNC_IDS,
                PostgresTable.VERTEX_IDS_AFTER_LINKING
        );
    }
}
