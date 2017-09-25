package com.dataloom.blocking;

import com.dataloom.linking.HazelcastBlockingService;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.UUID;

public class BlockingAggregator extends Aggregator<Map.Entry<GraphEntityPair, LinkingEntity>, Boolean>
        implements HazelcastInstanceAware {
    private           HazelcastBlockingService     blockingService;
    private           UUID                         graphId;
    private           Map<UUID, UUID>              entitySetIdsToSyncIds;
    private           Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn;
    private transient IAtomicLong                  asyncCallCounter;

    public BlockingAggregator(
            UUID graphId,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn ) {
        this( graphId, entitySetIdsToSyncIds, propertyTypesIndexedByFqn, null );
    }

    public BlockingAggregator(
            UUID graphId,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn,
            HazelcastBlockingService blockingService ) {
        this.graphId = graphId;
        this.entitySetIdsToSyncIds = entitySetIdsToSyncIds;
        this.propertyTypesIndexedByFqn = propertyTypesIndexedByFqn;
        this.blockingService = blockingService;
    }

    @Override public void accumulate( Map.Entry<GraphEntityPair, LinkingEntity> input ) {
        GraphEntityPair graphEntityPair = input.getKey();
        LinkingEntity linkingEntity = input.getValue();
        asyncCallCounter.getAndIncrement();
        blockingService
                .blockAndMatch( graphEntityPair, linkingEntity, entitySetIdsToSyncIds, propertyTypesIndexedByFqn );
    }

    @Override public void combine( Aggregator aggregator ) {
    }

    @Override public Boolean aggregate() {
        long count = asyncCallCounter.get();
        while ( count > 0 ) {
            try {
                Thread.sleep( 5000 );
                long newCount = asyncCallCounter.get();
                if ( newCount == count ) {
                    System.err.println( "Nothing is happening." );
                    return false;
                }
                count = newCount;
            } catch ( InterruptedException e ) {
                System.err.println( "Error occurred while waiting for matching to finish." );
            }
        }
        return true;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.asyncCallCounter = hazelcastInstance.getAtomicLong( graphId.toString() );
    }

    public UUID getGraphId() {
        return graphId;
    }

    public Map<UUID, UUID> getEntitySetIdsToSyncIds() {
        return entitySetIdsToSyncIds;
    }

    public Map<FullQualifiedName, UUID> getPropertyTypesIndexedByFqn() {
        return propertyTypesIndexedByFqn;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        BlockingAggregator that = (BlockingAggregator) o;

        if ( !graphId.equals( that.graphId ) )
            return false;
        return entitySetIdsToSyncIds.equals( that.entitySetIdsToSyncIds );
    }

    @Override public int hashCode() {
        int result = graphId.hashCode();
        result = 31 * result + entitySetIdsToSyncIds.hashCode();
        return result;
    }
}
