package com.dataloom.streams;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class StreamUtil {
    public static Stream<Row> stream( ResultSet rs ) {
        return StreamSupport.stream( rs.spliterator(), false );
    }
}
