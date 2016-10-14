package com.kryptnostic.datastore;

import java.util.Set;

/**
 * @author Ho Chung
 * Modified from Drew's Permission.java in mapstore
 * 
 */

public enum Permission {
	/**
	 * Alter means changing metadata of a type - for example, adding/removing property types to an entity type
	 * Owner allows granting/revoking of rights
	 */
    READ, WRITE, ALTER, OWNER;
	
    public static Permission[] permissions = values();

    public static Permission[] getValues() {
        return permissions;
    }
    
    public static int asNumber( Permission permission ){
    	switch( permission ){
    	    case READ:
    	    	return 1;
    	    case WRITE:
    	    	return 2;
    	    case ALTER:
    	    	return 4;
    	    case OWNER:
    	    	return 8;
    	    default:
    	    	return 0;
    	}
    }
    
    public static int asNumber( Set<Permission> permissions ){
    	return permissions.stream()
    			.mapToInt( permission -> Permission.asNumber(permission) )
    			.sum();
    }
}

