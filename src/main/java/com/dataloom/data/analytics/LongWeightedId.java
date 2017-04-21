package com.dataloom.data.analytics;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LongWeightedId implements Comparable<LongWeightedId> {
    private final UUID id;
    private final long weight;

    public LongWeightedId( UUID id, long weight ) {
        this.id = id;
        this.weight = weight;
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
