package com.dataloom.linking.util;

import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public enum PersonMetric {
    FIRST_NAME_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_METAPHONE( metaphone( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasFirstName( e, map ) ) ),
    FIRST_NAME_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasFirstName( e, map ) ) ),

    MIDDLE_NAME_STRING( jaroWinkler( ( e, map ) -> DelegatedStringSet.wrap( Sets.newHashSet() ) ) ),
    MIDDLE_NAME_METAPHONE( metaphone( ( e, map ) -> DelegatedStringSet.wrap( Sets.newHashSet() ) ) ),
    MIDDLE_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> DelegatedStringSet.wrap( Sets.newHashSet() ) ) ),
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

    private static final PersonMetric[]    metrics         = PersonMetric.values();
    private static final Set<PersonMetric> metricsList     = Sets.newHashSet( PersonMetric.values() );
    private static final DoubleMetaphone   doubleMetaphone = new DoubleMetaphone();

    private final MetricExtractor metric;

    PersonMetric( MetricExtractor metric ) {
        this.metric = metric;
    }

    private double extract(
            Map<UUID, DelegatedStringSet> lhs,
            Map<UUID, DelegatedStringSet> rhs,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return this.metric.extract( lhs, rhs, fqnToIdMap );
    }

    public static Double[] distance(
            Map<UUID, DelegatedStringSet> lhs,
            Map<UUID, DelegatedStringSet> rhs,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        Double[] result = new Double[ metrics.length ];
        metricsList.parallelStream().forEach( m -> {
            result[ m.ordinal() ] = m.extract( lhs, rhs, fqnToIdMap );
        } );
        return result;
    }

    public static double[] pDistance(
            Map<UUID, DelegatedStringSet> lhs,
            Map<UUID, DelegatedStringSet> rhs,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        double[] result = new double[ metrics.length ];
        metricsList.parallelStream().forEach( m -> {
            result[ m.ordinal() ] = m.extract( lhs, rhs, fqnToIdMap );
        } );
        return result;
    }

    public static MetricExtractor lhs( BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, Integer> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( lhs, fqnToIdMap );
    }

    public static MetricExtractor rhs( BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, Integer> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( rhs, fqnToIdMap );
    }

    public static MetricExtractor jaroWinkler(
            BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, DelegatedStringSet> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                false,
                true );
    }

    public static MetricExtractor metaphone(
            BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, DelegatedStringSet> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                true,
                false );
    }

    public static MetricExtractor metaphoneAlternate(
            BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, DelegatedStringSet> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                true,
                true );
    }

    public static double getMaxStringDistance(
            DelegatedStringSet lhs,
            DelegatedStringSet rhs,
            boolean useMetaphone,
            boolean alternate ) {
        double max = 0;
        for ( String s1 : lhs ) {
            for ( String s2 : rhs ) {
                double difference = getStringDistance( s1.toLowerCase(), s2.toLowerCase(), useMetaphone, alternate );
                if ( difference > max ) { max = difference; }
            }
        }
        return max;
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

}