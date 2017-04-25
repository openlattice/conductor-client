package com.dataloom.data.analytics;

import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class TopUtilizers {
    private final int                                   limit;
    private final ConcurrentSkipListSet<LongWeightedId> utilizers;

    public TopUtilizers( int limit ) {
        Preconditions.checkArgument( limit > 0, "Number of top utilizers must be non-zero" );
        this.limit = limit;
        this.utilizers = new ConcurrentSkipListSet<LongWeightedId>();
    }

    public void accumulate( LongWeightedId id ) {
        if ( utilizers.size() < limit ) {
            utilizers.add( id );
        } else {
            if ( utilizers.first().getWeight() < id.getWeight() ) {
                utilizers.add( id );
                utilizers.pollFirst();
            }
        }
    }

    public Stream<LongWeightedId> stream() {
        return utilizers.descendingSet().stream();
    }

    public void accumulate( UUID vertexId, long score ) {
        accumulate( new LongWeightedId( vertexId, score ) );
    }
}
