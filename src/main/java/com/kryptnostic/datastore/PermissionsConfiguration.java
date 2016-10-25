package com.kryptnostic.datastore;

public class PermissionsConfiguration {
    private void PermissionsConfiguration () {}
    
    /**
     * The configuration stores the required Permission for different actions
     */
    public static Permission UPSERT_ENTITY_TYPE = Permission.OWNER;
    public static Permission UPSERT_PROPERTY_TYPE = Permission.OWNER;
    public static Permission UPSERT_ENTITY_SET = Permission.OWNER;
    public static Permission DELETE_ENTITY_TYPE = Permission.OWNER;
    public static Permission DELETE_PROPERTY_TYPE = Permission.OWNER;
    public static Permission DELETE_ENTITY_SET = Permission.OWNER;
    public static Permission GET_ENTITY_TYPE = Permission.DISCOVER;
    public static Permission GET_PROPERTY_TYPE = Permission.DISCOVER;
    public static Permission GET_ENTITY_SET = Permission.DISCOVER;
    public static Permission ALTER_ENTITY_TYPE = Permission.OWNER;
    public static Permission ALTER_ENTITY_SET = Permission.OWNER;
    
    public static Permission ASSIGN_ENTITY_TO_ENTITY_SET = Permission.WRITE;    
    public static Permission IS_EXISTING_ENTITY_SET = Permission.DISCOVER;    
}
