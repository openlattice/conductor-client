package com.dataloom.linking.util;

import com.google.common.collect.Sets;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.UUID;

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

    public static DelegatedStringSet getValuesAsSet( Map<UUID, DelegatedStringSet> entity, UUID id ) {
        return ( entity.containsKey( id ) ) ? entity.get( id ) : DelegatedStringSet.wrap( Sets.newHashSet() );
    }

    public static int valueIsPresent( Map<UUID, DelegatedStringSet> entity, UUID propertyTypeId ) {
        if ( !entity.containsKey( propertyTypeId ) || entity.get( propertyTypeId ).size() == 0 )
            return 0;
        return 1;
    }

    public static DelegatedStringSet getFirstName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( FIRST_NAME_FQN ) );
    }

    public static int getHasFirstName( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( FIRST_NAME_FQN ) );
    }

    public static DelegatedStringSet getMiddleName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( MIDDLE_NAME_FQN ) );
    }

    public static int getHasMiddleName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( MIDDLE_NAME_FQN ) );
    }

    public static DelegatedStringSet getLastName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( LAST_NAME_FQN ) );
    }

    public static int getHasLastName( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( LAST_NAME_FQN ) );
    }

    public static DelegatedStringSet getSex( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( SEX_FQN ) );
    }

    public static int getHasSex( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( SEX_FQN ) );
    }

    public static DelegatedStringSet getRace( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( RACE_FQN ) );
    }

    public static int getHasRace( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( RACE_FQN ) );
    }

    public static DelegatedStringSet getEthnicity(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( ETHNICITY_FQN ) );
    }

    public static int getHasEthnicity( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( ETHNICITY_FQN ) );
    }

    public static DelegatedStringSet getDob( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( DOB_FQN ) );
    }

    public static int getHasDob( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( DOB_FQN ) );
    }

    public static DelegatedStringSet getIdentification(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( IDENTIFICATION_FQN ) );
    }

    public static int getHasIdentification(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( IDENTIFICATION_FQN ) );
    }

    public static DelegatedStringSet getSsn( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( SSN_FQN ) );
    }

    public static int getHasSsn( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( SSN_FQN ) );
    }

    public static DelegatedStringSet getAge( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( AGE_FQN ) );
    }

    public static int getHasAge( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( AGE_FQN ) );
    }

    public static DelegatedStringSet getXref( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( XREF_FQN ) );
    }

    public static int getHasXref( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( XREF_FQN ) );
    }

}
