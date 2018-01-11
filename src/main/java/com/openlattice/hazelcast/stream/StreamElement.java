package com.openlattice.hazelcast.stream;

import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class StreamElement<T> {
    private static final StreamElement EOF = new StreamElement( true, null );
    private final T       elem;
    private final boolean eof;

    protected StreamElement( @Nullable T elem ) {
        this( false, elem );
    }

    private StreamElement( boolean eof, T elem ) {
        this.elem = elem;
        this.eof = eof;
    }

    public boolean isEof() {
        return eof;
    }

    public T get() {
        if ( EOF == this ) {
            throw new NoSuchElementException( "Unable to get element corresponding to end of stream." );
        }
        return elem;
    }

    public static StreamElement eof() {
        return EOF;
    }
}
