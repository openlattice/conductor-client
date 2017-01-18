package com.kryptnostic.datastore.util;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.IMap;

public final class Util {
    private static final Logger logger = LoggerFactory.getLogger( Util.class );

    private Util() {}

    public static <T> T getFutureSafely( ListenableFuture<T> futurePropertyType ) {
        try {
            return futurePropertyType.get();
        } catch ( InterruptedException | ExecutionException e1 ) {
            logger.error( "Failed to load {} type",
                    futurePropertyType.getClass().getTypeParameters()[ 0 ].getTypeName() );
            return null;
        }
    }

    public static boolean wasLightweightTransactionApplied( ResultSet rs ) {
        Row row = rs.one();
        if ( row == null ) {
            return true;
        } else {
            return row.getBool( DatastoreConstants.APPLIED_FIELD );
        }
    }

    public static long getCount( ResultSet rs ) {
        return rs.one().getLong( DatastoreConstants.COUNT_FIELD );
    }

    public static boolean isCountNonZero( ResultSet rs ) {
        return getCount( rs ) > 0;
    }

    public static String toUnhyphenatedString( UUID uuid ) {
        return Long.toString( uuid.getLeastSignificantBits() ) + "_" + Long.toString( uuid.getMostSignificantBits() );
    }

    public static <T> Function<Row, T> transformSafelyFactory( Function<Row, T> f ) {
        return ( Row row ) -> transformSafely( row, f );

    }

    public static <T> T transformSafely( Row row, Function<Row, T> f ) {
        if ( row == null ) {
            return null;
        }
        return f.apply( row );
    }

    public static <K, V> V getSafely( IMap<K, V> m, K key ) {
        return m.get( key );
    }

    public static <K, V> V getSafely( Map<K, V> m, K key ) {
        return m.get( key );
    }

    public static <K, V> void deleteSafely( IMap<K, V> m, K key ) {
        m.delete( key );
    }

    public static <K, V> Function<K, V> getSafeMapper( IMap<K, V> m ) {
        return m::get;
    }

    public static <K, V> Consumer<? super K> safeDeleter( IMap<K, V> m ) {
        return m::delete;
    }

    public static <K, V> V removeSafely(
            IMap<K, V> fqns,
            K organizationId ) {
        return fqns.remove( organizationId );
    }
}
