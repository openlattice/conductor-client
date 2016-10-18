package com.kryptnostic.datastore;

import java.util.Set;

/**
 * @author Ho Chung Siu
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
    
    public int asNumber(){
    	return Permission.asNumber( this );
    }
    
    public int getPosition(){
    	return Permission.getPosition( this );
    }
    
    public static int asNumber( Permission permission ){
    	//WARNING: This function has to be in sync with getPosition
    	switch( permission ){
    	    case READ:
    	    	return 1;
    	    case WRITE:
    	    	return 2;
    	    case ALTER:
    	    	return 4;
    	    case OWNER: // Owner has rights to alter, write, read as well; 8+4+2+1 = 15.
    	    	return 15;
    	    default:
    	    	return 0;
    	}
    }
    
    public static int asNumber( Set<Permission> permissions ){
    	return permissions.stream()
    			.mapToInt( permission -> Permission.asNumber(permission) )
    			//TODO update this; sum is bad, because of the OWNER number above. You want bitwise or.
    			.sum();
    }
    
    public static int getPosition( Permission permission ){
    	//WARNING: This function has to be in sync with asNumber
    	switch( permission ){
    	    case READ:
    	    	return 0;
    	    case WRITE:
    	    	return 1;
    	    case ALTER:
    	    	return 2;
    	    case OWNER:
    	    	return 3;
    	    default:
    	    	return -1;
    	}
    }
}

