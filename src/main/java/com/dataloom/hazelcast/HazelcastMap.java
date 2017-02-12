/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
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
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.hazelcast;

import com.kryptnostic.conductor.rpc.odata.Tables;

public enum HazelcastMap {
    ACL_KEYS( Tables.ACL_KEYS ),
    AUDIT_EVENTS( Tables.AUDIT_EVENTS ),
    AUDIT_METRICS( Tables.AUDIT_METRICS ),
    ENTITY_EDGES( Tables.ENTITY_EDGES ),
    LINKING_EDGES( Tables.LINKING_EDGES ),
    PERMISSIONS( Tables.PERMISSIONS ),
    PERMISSIONS_REQUESTS_UNRESOLVED( Tables.PERMISSIONS_REQUESTS_UNRESOLVED ),
    PERMISSIONS_REQUESTS_RESOLVED( Tables.PERMISSIONS_REQUESTS_RESOLVED ),
    PROPERTY_TYPES( Tables.PROPERTY_TYPES ),
    ENTITY_TYPES( Tables.ENTITY_TYPES ),
    ENTITY_SETS( Tables.ENTITY_SETS ),
    NAMES( Tables.NAMES ),
    SCHEMAS( Tables.SCHEMAS ),
    TRUSTED_ORGANIZATIONS( Tables.ORGANIZATIONS ),
    VISIBILITY( Tables.ORGANIZATIONS ),
    TITLES( Tables.ORGANIZATIONS ),
    DESCRIPTIONS( Tables.ORGANIZATIONS ),
    ALLOWED_EMAIL_DOMAINS( Tables.ORGANIZATIONS ),
    MEMBERS( Tables.ORGANIZATIONS ),
    ROLES( Tables.ORGANIZATIONS ),
    USERS( null ),
    ENTITY_SET_TICKETS( null ),
    ENTITY_SET_PROPERTIES_TICKETS( null ),
    REQUESTS( Tables.REQUESTS ),
    SECURABLE_OBJECT_TYPES( Tables.PERMISSIONS )
    ;

    private final Tables table;

    private HazelcastMap( Tables table ) {
        this.table = table;
    }

    public Tables getTable() {
        return table;
    }
}
