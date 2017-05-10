package com.dataloom.graph.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.*;

/**
 * In memory representation for efficient aggregation queries.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class Neighborhood {
    // vertex id -> dst type id -> edge type id -> dst entity key id -> edge entity key id
    private final Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood;

    public Neighborhood( Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood ) {
        this.neighborhood = Preconditions.checkNotNull( neighborhood, "Neighborhood cannot be null" );
    }

    public Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> getNeighborhood() {
        return neighborhood;
    }

    public int count( Set<UUID> dstTypeIds, UUID edgeTypeId, Optional<UUID> dstEntityKeyId ) {
        return ( dstTypeIds.isEmpty() ? neighborhood.values().stream() : dstTypeIds.stream() )
                .map( dstTypeId -> neighborhood.getOrDefault( dstTypeId, ImmutableMap.of() ) )
                .map( dstEntityKeyIds -> dstEntityKeyIds.getOrDefault( edgeTypeId, ImmutableSetMultimap.of() ) )
                .mapToInt( edgeEntityKeyIds ->
                        dstEntityKeyId.<Collection<UUID>>transform( edgeEntityKeyIds::get )
                                .or( edgeEntityKeyIds.values() )
                                .size() )
                .sum();
    }

    public int count( Set<UUID> dstTypeIds, UUID edgeTypeId ) {
        return count( dstTypeIds, edgeTypeId, Optional.absent() );
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( !( o instanceof Neighborhood ) )
            return false;

        Neighborhood that = (Neighborhood) o;

        return neighborhood.equals( that.neighborhood );
    }

    @Override public int hashCode() {
        return neighborhood.hashCode();
    }

    @VisibleForTesting
    public static Neighborhood randomNeighborhood() {
        SetMultimap<UUID, UUID> sm1 = HashMultimap.create();
        SetMultimap<UUID, UUID> sm2 = HashMultimap.create();
        SetMultimap<UUID, UUID> sm3 = HashMultimap.create();

        for( int i = 0 ; i < 5; ++i  ) {
            sm1.put( UUID.randomUUID(), UUID.randomUUID() );
            sm2.put( UUID.randomUUID(), UUID.randomUUID() );
            sm3.put( UUID.randomUUID(), UUID.randomUUID() );
        }

        Map<UUID, SetMultimap<UUID, UUID>> m1 = new HashMap<>();
        Map<UUID, SetMultimap<UUID, UUID>> m2 = new HashMap<>();
        Map<UUID, SetMultimap<UUID, UUID>> m3 = new HashMap<>();

        for( int i = 0 ; i < 5; ++i  ) {
            m1.put(UUID.randomUUID(),sm1);
            m2.put(UUID.randomUUID(), sm2);
            m3.put(UUID.randomUUID(), sm3);
        }

        Map<UUID, Map<UUID, SetMultimap<UUID, UUID>>> neighborhood = new HashMap<>();

        for( int i = 0 ; i < 5; ++i  ) {
            neighborhood.put( UUID.randomUUID() , m1 );
            neighborhood.put( UUID.randomUUID() , m2 );
            neighborhood.put( UUID.randomUUID() , m3 );
        }

        return new Neighborhood( neighborhood );
    }
}
