package com.kryptnostic.conductor.rpc.odata;

public final class DatastoreConstants {
    private DatastoreConstants() {}

    /**
     * This property is deprecated and should be dynamically configured.
     */
    @Deprecated
    public static final String KEYSPACE             = "sparks";

    // TABLES
    public static final String ENTITY_SETS_TABLE        = "entity_sets";
    public static final String CONTAINERS_TABLE         = "containers";
    public static final String PRIMARY_NAMESPACE        = "agora";
    public static final String COUNT_FIELD              = "count";
    public static final String ENTITY_TYPES_TABLE       = "entity_types";
    public static final String PROPERTY_TYPES_TABLE     = "property_types";
    public static final String SCHEMAS_TABLE_PREFIX     = "schemas_";
    public static final String ENTITY_SET_MEMBERS_TABLE = "entity_set_members";
    
    // ACL TABLES
    public static final String ENTITY_TYPES_ROLES_ACLS_TABLE   = "entity_types_roles_acls";
    public static final String ENTITY_TYPES_USERS_ACLS_TABLE   = "entity_types_users_acls_by_entity_types";
    
    public static final String ENTITY_SETS_ROLES_ACLS_TABLE    = "entity_sets_roles_acls";
    public static final String ENTITY_SETS_USERS_ACLS_TABLE    = "entity_sets_users_acls";
    
    public static final String SCHEMAS_ACLS_TABLE        = "schema_acls";   

    public static final String PROPERTY_TYPES_IN_ENTITY_TYPES_ROLES_ACLS_TABLE = "property_types_in_entity_types_roles_acls";
    public static final String PROPERTY_TYPES_IN_ENTITY_TYPES_USERS_ACLS_TABLE = "property_types_in_entity_types_users_acls";
    
    public static final String PROPERTY_TYPES_IN_ENTITY_SETS_ROLES_ACLS_TABLE = "property_types_in_entity_sets_roles_acls";
    public static final String PROPERTY_TYPES_IN_ENTITY_SETS_USERS_ACLS_TABLE = "property_types_in_entity_sets_users_acls";
        
    // PARAMETERS
    public static final String APPLIED_FIELD        = "[applied]";
}
