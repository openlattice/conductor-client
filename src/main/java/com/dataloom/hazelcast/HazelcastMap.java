package com.dataloom.hazelcast;

import com.kryptnostic.conductor.rpc.odata.Tables;

public enum HazelcastMap {
    PROPERTY_TYPES( Tables.PROPERTY_TYPES ),
    ENTITY_TYPES( Tables.ENTITY_TYPES ),
    ENTITY_SETS( Tables.ENTITY_SETS ),
    TYPENAMES( Tables.TYPENAMES ),
    ACL_KEYS( Tables.FQNS ),
    FQNS( Tables.FQN_ACL_KEY ),
    SCHEMAS( Tables.SCHEMAS ),
    TRUSTED_ORGANIZATIONS( Tables.ORGANIZATIONS ),
    VISIBILITY( Tables.ORGANIZATIONS ),
    TITLES( Tables.ORGANIZATIONS ),
    DESCRIPTIONS( Tables.ORGANIZATIONS ),
    ALLOWED_EMAIL_DOMAINS( Tables.ORGANIZATIONS ),
    MEMBERS( Tables.ORGANIZATIONS ),
    ROLES( Tables.ORGANIZATIONS );

    private final Tables table;

    private HazelcastMap( Tables table ) {
        this.table = table;
    }

    public Tables getTable() {
        return table;
    }
}
