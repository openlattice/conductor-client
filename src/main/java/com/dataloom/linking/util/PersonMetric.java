package com.dataloom.linking.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.dataloom.linking.Entity;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 * 
 * 
 */
public enum PersonMetric {
    FIRST_NAME_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_METAPHONE( metaphone( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasFirstName( e, map ) ) ),
    FIRST_NAME_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasFirstName( e, map ) ) ),

    MIDDLE_NAME_STRING( jaroWinkler( ( e, map ) -> Sets.newHashSet() ) ),
    MIDDLE_NAME_METAPHONE( metaphone( ( e, map ) -> Sets.newHashSet() ) ),
    MIDDLE_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> Sets.newHashSet() ) ),
    MIDDLE_NAME_LHS_PRESENCE( lhs( ( e, map ) -> 0 ) ),
    MIDDLE_NAME_RHS_PRESENCE( rhs( ( e, map ) -> 0 ) ),

    LAST_NAME_STRINGG( jaroWinkler( ( e, map ) -> PersonProperties.getLastName( e, map ) ) ),
    LAST_NAME_METAPHONE( metaphone( ( e, map ) -> PersonProperties.getLastName( e, map ) ) ),
    LAST_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> PersonProperties.getLastName( e, map ) ) ),
    LAST_NAME_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasLastName( e, map ) ) ),
    LAST_NAME_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasLastName( e, map ) ) ),

    SSN_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getSsn( e, map ) ) ),
    SSN_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasSsn( e, map ) ) ),
    SSN_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasSsn( e, map ) ) ),

    SEX_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getSex( e, map ) ) ),
    SEX_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasSex( e, map ) ) ),
    SEX_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasSex( e, map ) ) ),

    DOB_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getDob( e, map ) ) ),
    DOB_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasDob( e, map ) ) ),
    DOB_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasDob( e, map ) ) ),

    RACE_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getRace( e, map ) ) ),
    RACE_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasRace( e, map ) ) ),
    RACE_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasRace( e, map ) ) ),

    ETHNICITY_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getEthnicity( e, map ) ) ),
    ETHNICITY_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasEthnicity( e, map ) ) ),
    ETHNICITY_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasEthnicity( e, map ) ) );

    private static final PersonMetric[]  metrics            = PersonMetric.values();
    private static final StructType      schema;
    private static final DoubleMetaphone doubleMetaphone    = new DoubleMetaphone();

    private static FullQualifiedName     FIRST_NAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    private static FullQualifiedName     MIDDLE_NAME_FQN    = new FullQualifiedName( "nc.PersonMiddleName" );
    private static FullQualifiedName     LAST_NAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    private static FullQualifiedName     SEX_FQN            = new FullQualifiedName( "nc.PersonSex" );
    private static FullQualifiedName     RACE_FQN           = new FullQualifiedName( "nc.PersonRace" );
    private static FullQualifiedName     ETHNICITY_FQN      = new FullQualifiedName( "nc.PersonEthnicity" );
    private static FullQualifiedName     DOB_FQN            = new FullQualifiedName( "nc.PersonBirthDate" );
    private static FullQualifiedName     IDENTIFICATION_FQN = new FullQualifiedName( "nc.SubjectIdentification" );
    private static FullQualifiedName     SSN_FQN            = new FullQualifiedName( "nc.ssn" );
    private static FullQualifiedName     AGE_FQN            = new FullQualifiedName( "person.age" );
    private static FullQualifiedName     XREF_FQN           = new FullQualifiedName( "justice.xref" );

    private static String                FIRST_NAME_ID;
    private static String                MIDDLE_NAME_ID;
    private static String                LAST_NAME_ID;
    private static String                SEX_ID;
    private static String                RACE_ID;
    private static String                ETHNICITY_ID;
    private static String                DOB_ID;
    private static String                IDENTIFICATION_ID;
    private static String                SSN_ID;
    private static String                AGE_ID;
    private static String                XREF_ID;

    static {
        List<StructField> fields = new ArrayList<>();
        for ( PersonMetric pm : metrics ) {
            StructField field = DataTypes.createStructField( pm.name(), DataTypes.DoubleType, true );
            fields.add( field );
        }
        schema = DataTypes.createStructType( fields );
    }

    private final MetricExtractor metric;

    PersonMetric( MetricExtractor metric ) {
        this.metric = metric;
    }

    private static void initializeIds( Map<FullQualifiedName, String> fqnToIdMap ) {
        FIRST_NAME_ID = fqnToIdMap.get( FIRST_NAME_FQN );
        MIDDLE_NAME_ID = fqnToIdMap.get( MIDDLE_NAME_FQN );
        LAST_NAME_ID = fqnToIdMap.get( LAST_NAME_FQN );
        SEX_ID = fqnToIdMap.get( SEX_FQN );
        RACE_ID = fqnToIdMap.get( RACE_FQN );
        ETHNICITY_ID = fqnToIdMap.get( ETHNICITY_FQN );
        DOB_ID = fqnToIdMap.get( DOB_FQN );
        IDENTIFICATION_ID = fqnToIdMap.get( IDENTIFICATION_FQN );
        SSN_ID = fqnToIdMap.get( SSN_FQN );
        AGE_ID = fqnToIdMap.get( AGE_FQN );
        XREF_ID = fqnToIdMap.get( XREF_FQN );
    }

    private double extract( Entity lhs, Entity rhs, Map<FullQualifiedName, String> fqnToIdMap ) {
        return this.metric.extract( lhs, rhs, fqnToIdMap );
    }

    public static StructType getSchema() {
        return schema;
    }

    public static Double[] distance( Entity lhs, Entity rhs, Map<FullQualifiedName, String> fqnToIdMap ) {
        Double[] result = new Double[ metrics.length ];
        for ( int i = 0; i < result.length; ++i ) {
            result[ i ] = metrics[ i ].extract( lhs, rhs, fqnToIdMap );
        }
        return result;
    }

    public static double[] pDistance( Entity lhs, Entity rhs, Map<FullQualifiedName, String> fqnToIdMap ) {
        double[] result = new double[ metrics.length ];
        for ( int i = 0; i < result.length; ++i ) {
            result[ i ] = metrics[ i ].extract( lhs, rhs, fqnToIdMap );
        }
        return result;
    }

    public static MetricExtractor lhs( BiFunction<Entity, Map<FullQualifiedName, String>, Integer> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( lhs, fqnToIdMap );
    }

    public static MetricExtractor rhs( BiFunction<Entity, Map<FullQualifiedName, String>, Integer> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( rhs, fqnToIdMap );
    }

    public static MetricExtractor jaroWinkler(
            BiFunction<Entity, Map<FullQualifiedName, String>, Set<String>> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                false,
                true );
    }

    public static MetricExtractor metaphone(
            BiFunction<Entity, Map<FullQualifiedName, String>, Set<String>> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                true,
                false );
    }

    public static MetricExtractor metaphoneAlternate(
            BiFunction<Entity, Map<FullQualifiedName, String>, Set<String>> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                true,
                true );
    }

    public static double getMaxStringDistance(
            Set<String> lhs,
            Set<String> rhs,
            boolean useMetaphone,
            boolean alternate ) {
        double max = 0;
        for ( String s1 : lhs ) {
            for ( String s2 : rhs ) {
                double difference = getStringDistance( s1, s2, useMetaphone, alternate );
                if ( difference > max ) max = difference;
            }
        }
        return max;
    }

    public static Set<String> getValuesAsSet( Entity entity, String propertyTypeId ) {
        Map<String, Object> entityDetails = entity.getProperties();
        if ( !entityDetails.containsKey( propertyTypeId ) ) return Sets.newHashSet();

        Object val = entityDetails.get( propertyTypeId );
        if ( val instanceof Collection<?> ) {
            return ( (Collection<?>) val ).stream().map( obj -> obj.toString() ).collect( Collectors.toSet() );
        }
        return ImmutableSet.of( val.toString() );
    }

    public static double getStringDistance( String lhs, String rhs, boolean useMetaphone, boolean alternate ) {
        if ( lhs == null ) {
            lhs = "";
        }

        if ( rhs == null ) {
            rhs = "";
        }

        if ( useMetaphone ) {
            if ( StringUtils.isNotBlank( lhs ) ) {
                lhs = doubleMetaphone.doubleMetaphone( lhs, alternate );
            }
            if ( StringUtils.isNotBlank( rhs ) ) {
                rhs = doubleMetaphone.doubleMetaphone( rhs, alternate );
            }
        }

        return StringUtils.getJaroWinklerDistance( lhs, rhs );
    }

    public static Set<String> getFirstName( Entity entity ) {
        return getValuesAsSet( entity, FIRST_NAME_ID );
    }

    public static Set<String> getMiddleName( Entity entity ) {
        return getValuesAsSet( entity, MIDDLE_NAME_ID );
    }

    public static Set<String> getLastName( Entity entity ) {
        return getValuesAsSet( entity, LAST_NAME_ID );
    }

    public static Set<String> getSex( Entity entity ) {
        return getValuesAsSet( entity, SEX_ID );
    }

    public static Set<String> getRace( Entity entity ) {
        return getValuesAsSet( entity, RACE_ID );
    }

    public static Set<String> getEthnicity( Entity entity ) {
        return getValuesAsSet( entity, ETHNICITY_ID );
    }

    public static Set<String> getDob( Entity entity ) {
        return getValuesAsSet( entity, DOB_ID );
    }

    public static Set<String> getIdentification( Entity entity ) {
        return getValuesAsSet( entity, IDENTIFICATION_ID );
    }

    public static Set<String> getSsn( Entity entity ) {
        return getValuesAsSet( entity, SSN_ID );
    }

    public static Set<String> getAge( Entity entity ) {
        return getValuesAsSet( entity, AGE_ID );
    }

    public static Set<String> getXref( Entity entity ) {
        return getValuesAsSet( entity, XREF_ID );
    }

}