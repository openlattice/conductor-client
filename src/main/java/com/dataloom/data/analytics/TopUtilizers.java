package com.dataloom.data.analytics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Stream;


/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class TopUtilizers {
    private final int                                   limit;
    private final PriorityBlockingQueue<LongWeightedId> utilizers;

    public TopUtilizers( int limit ) {
        Preconditions.checkArgument( limit > 0, "Number of top utilizers must be non-zero" );
        this.limit = limit;
        this.utilizers = new PriorityBlockingQueue<>( limit );
    }

    public void accumulate( LongWeightedId id ) {
        if ( utilizers.size() < limit ) {
            utilizers.add( id );
        } else {
            if ( utilizers.peek().getWeight() < id.getWeight() ) {
                utilizers.put( id );
                utilizers.poll();
            }
        }
    }

    public Stream<LongWeightedId> stream() {
        List<LongWeightedId> temp = Lists.newArrayList();
        while (utilizers.size() > 0) {
            temp.add( 0, utilizers.poll() );
        }
        return temp.stream();
    }

    public void accumulate( UUID vertexId, long score ) {
        accumulate( new LongWeightedId( vertexId, score ) );
    }
}
