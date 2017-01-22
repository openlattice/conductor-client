package com.dataloom.hazelcast;

import com.kryptnostic.conductor.rpc.odata.Tables;

public enum HazelcastMap {
    PERMISSIONS( Tables.PERMISSIONS ),
    PERMISSIONS_REQUESTS_UNRESOLVED( Tables.PERMISSIONS_REQUESTS_UNRESOLVED ),
    PERMISSIONS_REQUESTS_RESOLVED( Tables.PERMISSIONS_REQUESTS_RESOLVED ),
    PROPERTY_TYPES( Tables.PROPERTY_TYPES ),
    ENTITY_TYPES( Tables.ENTITY_TYPES ),
    ENTITY_SETS( Tables.ENTITY_SETS ),
    ACL_KEYS( Tables.ACL_KEYS ),
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
    ENTITY_SET_PROPERTIES_TICKETS(null);

    private final Tables table;

    private HazelcastMap( Tables table ) {
        this.table = table;
    }

    public Tables getTable() {
        return table;
    }
}
