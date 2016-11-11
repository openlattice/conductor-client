package com.kryptnostic.datastore;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * @author Ho Chung Modified from Drew's Permission.java in mapstore
 * 
 */

public enum Permission {
    /**
     * Alter means changing metadata of a type - for example, adding/removing property types to an entity type
     */
    DISCOVER,
    LINK,
    READ,
    WRITE,
    ALTER;

    private static Map<String, Permission> namesMap = new HashMap<String, Permission>( 6 );

    static {
        namesMap.put( "discover", Permission.DISCOVER );
        namesMap.put( "link", Permission.LINK );
        namesMap.put( "read", Permission.READ );
        namesMap.put( "write", Permission.WRITE );
        namesMap.put( "alter", Permission.ALTER );
    }

    @JsonCreator
    public static Permission forValue( String value ) {
        return namesMap.get( StringUtils.lowerCase( value ) );
    }
}
