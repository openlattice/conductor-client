package com.kryptnostic.conductor.rpc.odata;

import static com.kryptnostic.conductor.rpc.odata.DatastoreConstants.*;
public enum Tables {
    ENTITIES( "entities" ),
    ENTITY_SETS( ENTITY_SETS_TABLE ),
    ENTITY_TYPES(ENTITY_TYPES_TABLE ),
    PROPERTY_TYPE_LOOKUP( "property_type_lookup" ),
    ENTITY_TYPE_LOOKUP( "entity_type_lookup" ),
    ENTITY_SET_LOOKUP( "entity_set_lookup" ),
    PROPERTIES( "_properties" ),
    PROPERTY_TYPES( PROPERTY_TYPES_TABLE ),
    ENTITY_ID_TO_TYPE( "entity_id_to_type" ),
    ENTITY_SET_MEMBERS( ENTITY_SET_MEMBERS_TABLE ),
    ENTITY_TYPES_ROLES_ACLS( ENTITY_TYPES_ROLES_ACLS_TABLE ),
    ENTITY_TYPES_USERS_ACLS( ENTITY_TYPES_USERS_ACLS_TABLE ),
    ENTITY_SETS_ROLES_ACLS( ENTITY_SETS_ROLES_ACLS_TABLE ),
    ENTITY_SETS_USERS_ACLS( ENTITY_SETS_USERS_ACLS_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS( PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS( PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS( PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS_TABLE ),
    PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS( PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS_TABLE ),
    SCHEMAS_ACLS( SCHEMAS_ACLS_TABLE );
	
    private final String tableName;

    private Tables( String tableName ) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
