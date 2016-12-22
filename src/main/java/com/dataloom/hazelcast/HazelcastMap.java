package com.dataloom.hazelcast;

import com.kryptnostic.conductor.rpc.odata.Tables;

public enum HazelcastMap {
    PROPERTY_TYPES( Tables.PROPERTY_TYPES ),
    ENTITY_TYPES( Tables.ENTITY_TYPES ),
    ENTITY_SETS( Tables.ENTITY_SETS ),
    TYPENAMES( Tables.TYPENAMES ),
    ACL_KEYS( Tables.FQNS ),
    FQNS( Tables.FQN_ACL_KEY ),
    SCHEMAS( Tables.SCHEMAS );

    private final Tables table;

    private HazelcastMap( Tables table ) {
        this.table = table;
    }

    public Tables getTable() {
        return table;
    }
}
