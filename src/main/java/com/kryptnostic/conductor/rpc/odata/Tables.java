package com.kryptnostic.conductor.rpc.odata;

import static com.kryptnostic.conductor.rpc.odata.DatastoreConstants.*;
public enum Tables {
    ENTITIES( "entities" ),
    ENTITY_SETS( ENTITY_SETS_TABLE ),
    ENTITY_TYPES(ENTITY_TYPES_TABLE ),
    FQN_LOOKUP( "fqn_lookup" ),
    PROPERTIES( "_properties" ),
    PROPERTY_TYPES( PROPERTY_TYPES_TABLE ),
    SCHEMAS( SCHEMAS_TABLE ),
    ENTITY_ID_TO_TYPE( "entity_id_to_type" );

    private final String tableName;

    private Tables( String tableName ) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
