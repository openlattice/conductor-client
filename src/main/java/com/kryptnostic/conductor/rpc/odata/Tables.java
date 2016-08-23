package com.kryptnostic.conductor.rpc.odata;

public enum Tables {
    ENTITIES( "entities" ),
    ENTITY_SETS( "entity_sets" ),
    ENTITY_TYPES( "" ),
    PROPERTIES( "_properties" ),
    PROPERTY_TYPES( "" ),
    SCHEMAS( "" );

    private final String tableName;

    private Tables( String tableName ) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
