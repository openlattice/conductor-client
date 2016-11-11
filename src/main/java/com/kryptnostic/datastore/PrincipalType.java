package com.kryptnostic.datastore;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum PrincipalType {
    ROLE, USER;
    
    private static Map<String, PrincipalType> namesMap = new HashMap<String, PrincipalType>( 2 );

    static {
        namesMap.put( "role", PrincipalType.ROLE );
        namesMap.put( "user", PrincipalType.USER );
    }

    @JsonCreator
    public static PrincipalType forValue( String value ) {
        return namesMap.get( StringUtils.lowerCase( value ) );
    }
}
