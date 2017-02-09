package com.dataloom.hazelcast;

import com.kryptnostic.conductor.rpc.odata.Tables;

public enum HazelcastMap {
    ACL_KEYS( Tables.ACL_KEYS ),
    AUDIT_EVENTS( Tables.AUDIT_EVENTS ),
    AUDIT_METRICS( Tables.AUDIT_METRICS ),
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
