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
    DISCOVER, READ, WRITE, ALTER, OWNER;
	
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
    	    case DISCOVER:
    	    	return 1;
    	    case READ: // Being able to read means you already know the existence of this type: 2+1 = 3
    	    	return 3;
    	    case WRITE: // Being able to write means you already know the existence of this type: 4+1 = 5
    	    	return 5;
    	    case ALTER: // Being able to alter means you already know the existence of this type: 8+1 = 9
    	    	return 9;
    	    case OWNER: // Owner has rights to alter, write, read, discover as well; 16+8+4+2+1 = 31.
    	    	return 31;
    	    default:
    	    	return 0;
    	}
    }
    
    public static int asNumber( Set<Permission> permissions ){
    	return permissions.stream()
    			.mapToInt( permission -> Permission.asNumber(permission) )
    			//Union of permission -> bitwise or.
    			.reduce(0, (a,b) -> a & b );
    }
    
    public static int getPosition( Permission permission ){
    	//WARNING: This function has to be in sync with asNumber
    	switch( permission ){
    	    case DISCOVER:
    	    	return 0;
    	    case READ:
    	    	return 1;
    	    case WRITE:
    	    	return 2;
    	    case ALTER:
    	    	return 3;
    	    case OWNER:
    	    	return 4;
    	    default:
    	    	return -1;
    	}
    }
}

