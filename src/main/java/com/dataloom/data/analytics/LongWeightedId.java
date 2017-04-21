package com.dataloom.data.analytics;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LongWeightedId implements Comparable<LongWeightedId> {
    private final @Nonnull UUID id;
    private final long weight;

    public LongWeightedId( @Nonnull UUID id, long weight ) {
        this.id = id;
        this.weight = weight;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( !( o instanceof LongWeightedId ) )
            return false;

        LongWeightedId that = (LongWeightedId) o;

        if ( weight != that.weight )
            return false;
        return id.equals( that.id );
    }

    @Override public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (int) ( weight ^ ( weight >>> 32 ) );
        return result;
    }

    @Override
    public int compareTo( LongWeightedId o ) {
        return Long.compare( weight, o.weight );
    }

    public long getWeight() {
        return weight;
    }

    public UUID getId() {
        return id;
    }
}
