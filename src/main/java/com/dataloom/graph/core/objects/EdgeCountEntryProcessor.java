package com.dataloom.graph.core.objects;

import com.dataloom.graph.core.Neighborhood;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class EdgeCountEntryProcessor extends AbstractRhizomeEntryProcessor<UUID, Neighborhood, Integer> {
    private final UUID associationTypeId;
    private final Set<UUID> neighborTypeIds;

    public EdgeCountEntryProcessor( UUID associationTypeId, Set<UUID> neighborTypeIds ) {
        this.associationTypeId = associationTypeId;
        this.neighborTypeIds = neighborTypeIds;
    }

    @Override
    public Integer process( Map.Entry<UUID, Neighborhood> entry ) {
        Neighborhood n = entry.getValue();
        if( n == null ) {
            return 0;
        }

        return n.count( neighborTypeIds, associationTypeId );
    }

    public UUID getAssociationTypeId() {
        return associationTypeId;
    }

    public Set<UUID> getNeighborTypeIds() {
        return neighborTypeIds;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( !( o instanceof EdgeCountEntryProcessor ) )
            return false;

        EdgeCountEntryProcessor that = (EdgeCountEntryProcessor) o;

        if ( !associationTypeId.equals( that.associationTypeId ) )
            return false;
        return neighborTypeIds.equals( that.neighborTypeIds );
    }

    @Override public int hashCode() {
        int result = associationTypeId.hashCode();
        result = 31 * result + neighborTypeIds.hashCode();
        return result;
    }
}
