package com.dataloom.linking.aggregators;

import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.linking.HazelcastMergingService;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICountDownLatch;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.springframework.scheduling.annotation.Async;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class MergeVertexAggregator extends Aggregator<Map.Entry<LinkingVertexKey, LinkingVertex>, Void>
        implements HazelcastInstanceAware {

    private           UUID                            graphId;
    private           UUID                            syncId;
    private           Map<UUID, Set<UUID>>            propertyTypeIdsByEntitySet;
    private           Map<UUID, PropertyType>         propertyTypesById;
    private           Set<UUID>                       propertyTypesToPopulate;
    private           Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet;
    private transient ICountDownLatch                 countDownLatch;
    private HazelcastMergingService         mergingService;

    private final int MAX_FAILED_CONSEC_ATTEMPTS = 5;

    public MergeVertexAggregator(
            UUID graphId,
            UUID syncId,
            Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet,
            Map<UUID, PropertyType> propertyTypesById,
            Set<UUID> propertyTypesToPopulate,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet ) {
        this( graphId, syncId, propertyTypeIdsByEntitySet, propertyTypesById, propertyTypesToPopulate, authorizedPropertiesWithDataTypeForLinkedEntitySet, null );
    }

    public MergeVertexAggregator(
            UUID graphId,
            UUID syncId,
            Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet,
            Map<UUID, PropertyType> propertyTypesById,
            Set<UUID> propertyTypesToPopulate,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet,
            HazelcastMergingService mergingService ) {
        this.graphId = graphId;
        this.syncId = syncId;
        this.propertyTypeIdsByEntitySet = propertyTypeIdsByEntitySet;
        this.propertyTypesById = propertyTypesById;
        this.propertyTypesToPopulate = propertyTypesToPopulate;
        this.authorizedPropertiesWithDataTypeForLinkedEntitySet = authorizedPropertiesWithDataTypeForLinkedEntitySet;
        this.mergingService = mergingService;
    }

    @Override public void accumulate( Map.Entry<LinkingVertexKey, LinkingVertex> input ) {
        mergingService.mergeEntity( input.getValue().getEntityKeys(),
                graphId,
                syncId,
                propertyTypeIdsByEntitySet,
                propertyTypesById,
                propertyTypesToPopulate,
                authorizedPropertiesWithDataTypeForLinkedEntitySet );
    }

    @Override public void combine( Aggregator aggregator ) {

    }

    @Override public Void aggregate() {
        int numConsecFailures = 0;
        long count = countDownLatch.getCount();
        while ( count > 0 && numConsecFailures < MAX_FAILED_CONSEC_ATTEMPTS ) {
            try {
                Thread.sleep( 5000 );
                long newCount = countDownLatch.getCount();
                if ( newCount == count ) {
                    System.err.println( "Nothing is happening." );
                    numConsecFailures++;
                } else
                    numConsecFailures = 0;
                count = newCount;
            } catch ( InterruptedException e ) {
                System.err.println( "Error occurred while waiting for linking vertices to merge." );
            }
        }
        return null;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.countDownLatch = hazelcastInstance.getCountDownLatch( graphId.toString() );
    }

    public UUID getGraphId() {
        return graphId;
    }

    public UUID getSyncId() {
        return syncId;
    }

    public Map<UUID, Set<UUID>> getPropertyTypeIdsByEntitySet() {
        return propertyTypeIdsByEntitySet;
    }

    public Map<UUID, PropertyType> getPropertyTypesById() {
        return propertyTypesById;
    }

    public Set<UUID> getPropertyTypesToPopulate() {
        return propertyTypesToPopulate;
    }

    public Map<UUID, EdmPrimitiveTypeKind> getAuthorizedPropertiesWithDataTypeForLinkedEntitySet() {
        return authorizedPropertiesWithDataTypeForLinkedEntitySet;
    }
}
