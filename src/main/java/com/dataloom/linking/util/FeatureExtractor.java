package com.dataloom.linking.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.linking.Entity;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

public class FeatureExtractor {

    private static FullQualifiedName FIRSTNAME_FQN      = new FullQualifiedName( "nc.PersonGivenName" );
    private static FullQualifiedName MIDDLENAME_FQN     = new FullQualifiedName( "nc.PersonMiddleName" );
    private static FullQualifiedName LASTNAME_FQN       = new FullQualifiedName( "nc.PersonSurName" );
    private static FullQualifiedName SEX_FQN            = new FullQualifiedName( "nc.PersonSex" );
    private static FullQualifiedName RACE_FQN           = new FullQualifiedName( "nc.PersonRace" );
    private static FullQualifiedName ETHNICITY_FQN      = new FullQualifiedName( "nc.PersonEthnicity" );
    private static FullQualifiedName DOB_FQN            = new FullQualifiedName( "nc.PersonBirthDate" );
    private static FullQualifiedName IDENTIFICATION_FQN = new FullQualifiedName( "nc.SubjectIdentification" );
    private static FullQualifiedName SSN_FQN            = new FullQualifiedName( "nc.ssn" );
    private static FullQualifiedName AGE_FQN            = new FullQualifiedName( "person.age" );
    private static FullQualifiedName XREF_FQN           = new FullQualifiedName( "justice.xref" );

    private static DoubleMetaphone   doubleMetaphone    = new DoubleMetaphone();

    public static double getStringDistance( Set<Object> s1, Set<Object> s2, boolean shouldUseMetaphone ) {
        double minDistance = Double.MAX_VALUE;

        for ( Object o1 : s1 ) {
            for ( Object o2 : s2 ) {
                String str1 = o1.toString();
                String str2 = o2.toString();
                if ( shouldUseMetaphone ) {
                    str1 = doubleMetaphone.encode( str1 );
                    str2 = doubleMetaphone.encode( str2 );
                }
                double dist = 1 - StringUtils.getJaroWinklerDistance( str1, str2 );
                if ( dist < minDistance ) minDistance = dist;

            }
        }
        if ( minDistance == Double.MAX_VALUE ) minDistance = 0;
        return minDistance;
    }

    public static double getPresenceValue( Set<Object> s1, Set<Object> s2 ) {
        if ( s1.isEmpty() == s2.isEmpty() ) return Double.valueOf( 1 );
        return 0;
    }

    public static double[] getEntityDistance(
            SetMultimap<FullQualifiedName, Object> p1,
            SetMultimap<FullQualifiedName, Object> p2 ) {
        Set<Object> firstName1 = p1.get( FIRSTNAME_FQN );
        Set<Object> firstName2 = p2.get( FIRSTNAME_FQN );

        Set<Object> middleName1 = p1.get( MIDDLENAME_FQN );
        Set<Object> middleName2 = p2.get( MIDDLENAME_FQN );

        Set<Object> lastName1 = p1.get( LASTNAME_FQN );
        Set<Object> lastName2 = p2.get( LASTNAME_FQN );

        Set<Object> sex1 = p1.get( SEX_FQN );
        Set<Object> sex2 = p2.get( SEX_FQN );

        Set<Object> race1 = p1.get( RACE_FQN );
        Set<Object> race2 = p2.get( RACE_FQN );

        Set<Object> ethnicity1 = p1.get( ETHNICITY_FQN );
        Set<Object> ethnicity2 = p2.get( ETHNICITY_FQN );

        Set<Object> dob1 = p1.get( DOB_FQN );
        Set<Object> dob2 = p2.get( DOB_FQN );

        Set<Object> id1 = p1.get( IDENTIFICATION_FQN );
        Set<Object> id2 = p2.get( IDENTIFICATION_FQN );

        Set<Object> ssn1 = p1.get( SSN_FQN );
        Set<Object> ssn2 = p2.get( SSN_FQN );

        Set<Object> age1 = p1.get( AGE_FQN );
        Set<Object> age2 = p2.get( AGE_FQN );

        Set<Object> xref1 = p1.get( XREF_FQN );
        Set<Object> xref2 = p2.get( XREF_FQN );

        double[] result = new double[ 26 ];
        result[ PersonFeatureTypes.BIAS.ordinal() ] = 1;

        result[ PersonFeatureTypes.FIRST_NAME_STRING.ordinal() ] = getStringDistance( firstName1, firstName2, false );
        result[ PersonFeatureTypes.FIRST_NAME_METAPHONE.ordinal() ] = getStringDistance( firstName1, firstName2, true );
        result[ PersonFeatureTypes.FIRST_NAME_PRESENCE.ordinal() ] = getPresenceValue( firstName1, firstName2 );

        result[ PersonFeatureTypes.MIDDLE_NAME_STRING.ordinal() ] = getStringDistance( middleName1,
                middleName2,
                false );
        result[ PersonFeatureTypes.MIDDLE_NAME_METAPHONE.ordinal() ] = getStringDistance( middleName1,
                middleName2,
                true );
        result[ PersonFeatureTypes.MIDDLE_NAME_PRESENCE.ordinal() ] = getPresenceValue( middleName1, middleName2 );

        result[ PersonFeatureTypes.LAST_NAME_STRING.ordinal() ] = getStringDistance( lastName1, lastName2, false );
        result[ PersonFeatureTypes.LAST_NAME_METAPHONE.ordinal() ] = getStringDistance( lastName1, lastName2, true );
        result[ PersonFeatureTypes.LAST_NAME_PRESENCE.ordinal() ] = getPresenceValue( lastName1, lastName2 );

        result[ PersonFeatureTypes.SEX_STRING.ordinal() ] = getStringDistance( sex1, sex2, false );
        result[ PersonFeatureTypes.SEX_PRESENCE.ordinal() ] = getPresenceValue( sex1, sex2 );

        result[ PersonFeatureTypes.RACE_STRING.ordinal() ] = getStringDistance( race1, race2, false );
        result[ PersonFeatureTypes.RACE_PRESENCE.ordinal() ] = getPresenceValue( race1, race2 );

        result[ PersonFeatureTypes.ETHNICITY_STRING.ordinal() ] = getStringDistance( ethnicity1, ethnicity2, false );
        result[ PersonFeatureTypes.ETHNICITY_PRESENCE.ordinal() ] = getPresenceValue( ethnicity1, ethnicity2 );

        result[ PersonFeatureTypes.DOB_STRING.ordinal() ] = getStringDistance( dob1, dob2, false );
        result[ PersonFeatureTypes.DOB_PRESENCE.ordinal() ] = getPresenceValue( dob1, dob2 );

        result[ PersonFeatureTypes.IDENTIFICATION_STRING.ordinal() ] = getStringDistance( id1, id2, false );
        result[ PersonFeatureTypes.IDENTIFICATION_PRESENCE.ordinal() ] = getPresenceValue( id1, id2 );

        result[ PersonFeatureTypes.SSN_STRING.ordinal() ] = getStringDistance( ssn1, ssn2, false );
        result[ PersonFeatureTypes.SSN_PRESENCE.ordinal() ] = getPresenceValue( ssn1, ssn2 );

        result[ PersonFeatureTypes.AGE_STRING.ordinal() ] = getStringDistance( age1, age2, false );
        result[ PersonFeatureTypes.AGE_PRESENCE.ordinal() ] = getPresenceValue( age1, age2 );

        result[ PersonFeatureTypes.XREF_STRING.ordinal() ] = getStringDistance( xref1, xref2, false );
        result[ PersonFeatureTypes.XREF_PRESENCE.ordinal() ] = getPresenceValue( xref1, xref2 );

        return result;
    }

    private static Set<Object> getValueAsSet(
            Map<String, Object> e,
            FullQualifiedName fqn,
            Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn ) {
        if ( !propertyTypeIdIndexedByFqn.containsKey( fqn ) ) return Sets.newHashSet();

        String propertyTypeId = propertyTypeIdIndexedByFqn.get( fqn );
        if ( !e.containsKey( propertyTypeId ) ) return Sets.newHashSet();

        Object val = e.get( propertyTypeId );
        if ( val instanceof Collection<?> ) {
            return ( (Collection<?>) val ).stream().map( obj -> obj.toString() ).collect( Collectors.toSet() );
        }
        return ImmutableSet.of( val.toString() );
    }

    public static double getEntityDiffForWeights(
            UnorderedPair<Entity> entityPair,
            double[] weights,
            Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn ) {
        List<Entity> pairAsList = entityPair.getAsList();
        Map<String, Object> e1 = pairAsList.get( 0 ).getProperties();
        Map<String, Object> e2 = pairAsList.get( 1 ).getProperties();

        Set<Object> firstName1 = getValueAsSet( e1, FIRSTNAME_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> firstName2 = getValueAsSet( e2, FIRSTNAME_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> middleName1 = getValueAsSet( e1, MIDDLENAME_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> middleName2 = getValueAsSet( e2, MIDDLENAME_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> lastName1 = getValueAsSet( e1, LASTNAME_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> lastName2 = getValueAsSet( e2, LASTNAME_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> sex1 = getValueAsSet( e1, SEX_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> sex2 = getValueAsSet( e2, SEX_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> race1 = getValueAsSet( e1, RACE_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> race2 = getValueAsSet( e2, RACE_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> ethnicity1 = getValueAsSet( e1, ETHNICITY_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> ethnicity2 = getValueAsSet( e2, ETHNICITY_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> dob1 = getValueAsSet( e1, DOB_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> dob2 = getValueAsSet( e2, DOB_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> id1 = getValueAsSet( e1, IDENTIFICATION_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> id2 = getValueAsSet( e2, IDENTIFICATION_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> ssn1 = getValueAsSet( e1, SSN_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> ssn2 = getValueAsSet( e2, SSN_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> age1 = getValueAsSet( e1, AGE_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> age2 = getValueAsSet( e2, AGE_FQN, propertyTypeIdIndexedByFqn );

        Set<Object> xref1 = getValueAsSet( e1, XREF_FQN, propertyTypeIdIndexedByFqn );
        Set<Object> xref2 = getValueAsSet( e2, XREF_FQN, propertyTypeIdIndexedByFqn );

        double result = 0.0;

        result += weights[ PersonFeatureTypes.BIAS.ordinal() ];

        result += weights[ PersonFeatureTypes.FIRST_NAME_STRING.ordinal() ]
                * getStringDistance( firstName1, firstName2, false );
        result += weights[ PersonFeatureTypes.FIRST_NAME_METAPHONE.ordinal() ]
                * getStringDistance( firstName1, firstName2, true );
        result += weights[ PersonFeatureTypes.FIRST_NAME_PRESENCE.ordinal() ]
                * getPresenceValue( firstName1, firstName2 );

        result += weights[ PersonFeatureTypes.MIDDLE_NAME_STRING.ordinal() ] * getStringDistance( middleName1,
                middleName2,
                false );
        result += weights[ PersonFeatureTypes.MIDDLE_NAME_METAPHONE.ordinal() ] * getStringDistance( middleName1,
                middleName2,
                true );
        result += weights[ PersonFeatureTypes.MIDDLE_NAME_PRESENCE.ordinal() ]
                * getPresenceValue( middleName1, middleName2 );

        result += weights[ PersonFeatureTypes.LAST_NAME_STRING.ordinal() ]
                * getStringDistance( lastName1, lastName2, false );
        result += weights[ PersonFeatureTypes.LAST_NAME_METAPHONE.ordinal() ]
                * getStringDistance( lastName1, lastName2, true );
        result += weights[ PersonFeatureTypes.LAST_NAME_PRESENCE.ordinal() ] * getPresenceValue( lastName1, lastName2 );

        result += weights[ PersonFeatureTypes.SEX_STRING.ordinal() ] * getStringDistance( sex1, sex2, false );
        result += weights[ PersonFeatureTypes.SEX_PRESENCE.ordinal() ] * getPresenceValue( sex1, sex2 );

        result += weights[ PersonFeatureTypes.RACE_STRING.ordinal() ] * getStringDistance( race1, race2, false );
        result += weights[ PersonFeatureTypes.RACE_PRESENCE.ordinal() ] * getPresenceValue( race1, race2 );

        result += weights[ PersonFeatureTypes.ETHNICITY_STRING.ordinal() ]
                * getStringDistance( ethnicity1, ethnicity2, false );
        result += weights[ PersonFeatureTypes.ETHNICITY_PRESENCE.ordinal() ]
                * getPresenceValue( ethnicity1, ethnicity2 );

        result += weights[ PersonFeatureTypes.DOB_STRING.ordinal() ] * getStringDistance( dob1, dob2, false );
        result += weights[ PersonFeatureTypes.DOB_PRESENCE.ordinal() ] * getPresenceValue( dob1, dob2 );

        // result += weights[ PersonFeatureTypes.IDENTIFICATION_STRING.ordinal() ] * getStringDistance( id1, id2, false
        // );
        // result += weights[ PersonFeatureTypes.IDENTIFICATION_PRESENCE.ordinal() ] * getPresenceValue( id1, id2 );

        result += weights[ PersonFeatureTypes.SSN_STRING.ordinal() ] * getStringDistance( ssn1, ssn2, false );
        result += weights[ PersonFeatureTypes.SSN_PRESENCE.ordinal() ] * getPresenceValue( ssn1, ssn2 );

        result += weights[ PersonFeatureTypes.AGE_STRING.ordinal() ] * getStringDistance( age1, age2, false );
        result += weights[ PersonFeatureTypes.AGE_PRESENCE.ordinal() ] * getPresenceValue( age1, age2 );

        result += weights[ PersonFeatureTypes.XREF_STRING.ordinal() ] * getStringDistance( xref1, xref2, false );
        result += weights[ PersonFeatureTypes.XREF_PRESENCE.ordinal() ] * getPresenceValue( xref1, xref2 );

        return Math.pow( 2.0, result ) / 10.0;
    }

}
