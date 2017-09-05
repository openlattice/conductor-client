package com.dataloom.linking.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.linking.Entity;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class PersonProperties {

    private static FullQualifiedName FIRST_NAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    private static FullQualifiedName MIDDLE_NAME_FQN    = new FullQualifiedName( "nc.PersonMiddleName" );
    private static FullQualifiedName LAST_NAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    private static FullQualifiedName SEX_FQN            = new FullQualifiedName( "nc.PersonSex" );
    private static FullQualifiedName RACE_FQN           = new FullQualifiedName( "nc.PersonRace" );
    private static FullQualifiedName ETHNICITY_FQN      = new FullQualifiedName( "nc.PersonEthnicity" );
    private static FullQualifiedName DOB_FQN            = new FullQualifiedName( "nc.PersonBirthDate" );
    private static FullQualifiedName IDENTIFICATION_FQN = new FullQualifiedName( "nc.SubjectIdentification" );
    private static FullQualifiedName SSN_FQN            = new FullQualifiedName( "nc.ssn" );
    private static FullQualifiedName AGE_FQN            = new FullQualifiedName( "person.age" );
    private static FullQualifiedName XREF_FQN           = new FullQualifiedName( "justice.xref" );

    public static Set<String> getValuesAsSet( Entity entity, String propertyTypeId ) {
        Map<String, Object> entityDetails = entity.getProperties();
        if ( !entityDetails.containsKey( propertyTypeId ) ) return Sets.newHashSet();

        Object val = entityDetails.get( propertyTypeId );
        if ( val instanceof Collection<?> ) {
            return ( (Collection<?>) val ).stream().map( obj -> obj.toString() ).collect( Collectors.toSet() );
        }
        return ImmutableSet.of( val.toString() );
    }

    public static int valueIsPresent( Entity entity, String propertyTypeId ) {
        Map<String, Object> entityDetails = entity.getProperties();
        if ( !entityDetails.containsKey( propertyTypeId ) ) return 0;
        Object val = entityDetails.get( propertyTypeId );
        if ( val instanceof Collection<?> ) return ( ( (Collection<?>) val ).size() > 0 ) ? 1 : 0;
        return ( val.toString().length() > 0 ) ? 1 : 0;
    }

    public static Set<String> getFirstName( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( FIRST_NAME_FQN ) );
    }

    public static int getHasFirstName( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( FIRST_NAME_FQN ) );
    }

    public static Set<String> getMiddleName( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( MIDDLE_NAME_FQN ) );
    }

    public static int getHasMiddleName( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( MIDDLE_NAME_FQN ) );
    }

    public static Set<String> getLastName( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( LAST_NAME_FQN ) );
    }

    public static int getHasLastName( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( LAST_NAME_FQN ) );
    }

    public static Set<String> getSex( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( SEX_FQN ) );
    }

    public static int getHasSex( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( SEX_FQN ) );
    }

    public static Set<String> getRace( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( RACE_FQN ) );
    }

    public static int getHasRace( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( RACE_FQN ) );
    }

    public static Set<String> getEthnicity( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( ETHNICITY_FQN ) );
    }

    public static int getHasEthnicity( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( ETHNICITY_FQN ) );
    }

    public static Set<String> getDob( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( DOB_FQN ) );
    }

    public static int getHasDob( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( DOB_FQN ) );
    }

    public static Set<String> getIdentification( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( IDENTIFICATION_FQN ) );
    }

    public static int getHasIdentification( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( IDENTIFICATION_FQN ) );
    }

    public static Set<String> getSsn( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( SSN_FQN ) );
    }

    public static int getHasSsn( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( SSN_FQN ) );
    }

    public static Set<String> getAge( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( AGE_FQN ) );
    }

    public static int getHasAge( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( AGE_FQN ) );
    }

    public static Set<String> getXref( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( XREF_FQN ) );
    }

    public static int getHasXref( Entity entity, Map<FullQualifiedName, String> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( XREF_FQN ) );
    }

}
