package com.kryptnostic.conductor.rpc.odata;

public enum Tables {
    ENTITIES( "entities" ),
    ENTITY_SETS( "entity_sets" ),
    ENTITY_TYPES( "entity_types" ),
    FQN_LOOKUP( "fqn_lookup" ),
    PROPERTIES( "_properties" ),
    PROPERTY_TYPES( "property_types" ),
    SCHEMAS( "schemas" ),
    ENTITY_ID_TO_TYPE( "entity_id_to_type" );

    private final String tableName;

    private Tables( String tableName ) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
