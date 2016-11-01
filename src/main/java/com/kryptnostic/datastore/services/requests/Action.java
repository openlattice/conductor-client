package com.kryptnostic.datastore.services.requests;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum Action {
    ADD,
    REMOVE,
    SET;

    private static Map<String, Action> namesMap = new HashMap<>( 3 );

    static {
        namesMap.put( "add", Action.ADD );
        namesMap.put( "remove", Action.REMOVE );
        namesMap.put( "set", Action.SET );
    }

    @JsonCreator
    public static Action forValue( String value ) {
        return namesMap.get( StringUtils.lowerCase( value ) );
    }
}
