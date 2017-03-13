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

import com.kryptnostic.conductor.rpc.odata.Table;

public enum HazelcastMap {
    ACL_KEYS( Table.ACL_KEYS ),
    AUDIT_EVENTS( Table.AUDIT_EVENTS ),
    AUDIT_METRICS( Table.AUDIT_METRICS ),
    COMPLEX_TYPES( Table.COMPLEX_TYPES ),
    ENTITY_EDGES( Table.ENTITY_EDGES ),
    ENUM_TYPES( Table.ENUM_TYPES ),
    LINKED_ENTITY_TYPES( Table.LINKED_ENTITY_TYPES ),
    LINKED_ENTITY_SETS( Table.LINKED_ENTITY_SETS ),
    LINKED_ENTITIES( Table.LINKED_ENTITIES ),
    LINKING_EDGES( Table.WEIGHTED_LINKING_EDGES ),
    LINKING_ENTITY_VERTICES( Table.LINKING_ENTITY_VERTICES ),
    LINKING_VERTICES( Table.LINKING_VERTICES ),
    PERMISSIONS( Table.PERMISSIONS ),
    PERMISSIONS_REQUESTS_UNRESOLVED( Table.PERMISSIONS_REQUESTS_UNRESOLVED ),
    PERMISSIONS_REQUESTS_RESOLVED( Table.PERMISSIONS_REQUESTS_RESOLVED ),
    PROPERTY_TYPES( Table.PROPERTY_TYPES ),
    ENTITY_TYPES( Table.ENTITY_TYPES ),
    ENTITY_SETS( Table.ENTITY_SETS ),
    NAMES( Table.NAMES ),
    SCHEMAS( Table.SCHEMAS ),
    TRUSTED_ORGANIZATIONS( Table.ORGANIZATIONS ),
    VISIBILITY( Table.ORGANIZATIONS ),
    ORGANIZATIONS_TITLES( Table.ORGANIZATIONS ),
    ORGANIZATIONS_DESCRIPTIONS( Table.ORGANIZATIONS ),
    ALLOWED_EMAIL_DOMAINS( Table.ORGANIZATIONS ),
    ORGANIZATIONS_MEMBERS( Table.ORGANIZATIONS ),
    ORGANIZATIONS_ROLES( Table.ORGANIZATIONS_ROLES ),
    USERS_WITH_ROLE( Table.ORGANIZATIONS_ROLES ),
    USERS( null ),
    ENTITY_SET_TICKETS( null ),
    ENTITY_SET_PROPERTIES_TICKETS( null ),
    REQUESTS( Table.REQUESTS ),
    SECURABLE_OBJECT_TYPES( Table.PERMISSIONS ),
    TOKEN_ACCEPTANCE_TIME( null )
    ;

    private final Table table;

    private HazelcastMap( Table table ) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

}
