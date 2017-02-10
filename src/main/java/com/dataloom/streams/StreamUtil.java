package com.dataloom.streams;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class StreamUtil {
    public static <T> Stream<T> stream( Iterable<T> rs ) {
        return StreamSupport.stream( rs.spliterator(), false );
    }

    /**
     * Useful adapter for {@code Iterables#transform(Iterable, com.google.common.base.Function)} that allows lazy
     * evaluation of result set future. See the same function in AuthorizationUtils as well.
     * 
     * @param rsf The result set future to make a lazy evaluated iterator
     * @return The lazy evaluatable iterable
     */
    public static Iterable<Row> makeLazy( ResultSetFuture rsf ) {
        return getRowsAndFlatten( Stream.of( rsf ) )::iterator;
    }

    public static Stream<Row> getRowsAndFlatten( Stream<ResultSetFuture> stream ) {
        return stream.map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream );
    }
}
