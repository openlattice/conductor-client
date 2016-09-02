package com.kryptnostic.datastore.util;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;

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
}
