package com.dataloom.merging;

import com.dataloom.data.DatasourceManager;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.graph.mapstores.PostgresEdgeMapstore;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.LinkingVertex;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.aggregators.CountVerticesAggregator;
import com.dataloom.linking.aggregators.MergeEdgeAggregator;
import com.dataloom.linking.aggregators.MergeVertexAggregator;
import com.dataloom.linking.predicates.LinkingPredicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.services.EdmManager;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DistributedMerger {
    private static final Logger logger = LoggerFactory.getLogger( DistributedMerger.class );

    private final IMap<LinkingVertexKey, LinkingVertex> linkingVertices;
    private final IMap<EdgeKey, LoomEdge>               edges;
    private final HazelcastListingService               listingService;
    private final EdmManager                            dms;
    private final DatasourceManager                     datasourceManager;
    private final HazelcastInstance                     hazelcast;

    public DistributedMerger(
            HazelcastInstance hazelcast,
            HazelcastListingService listingService,
            EdmManager dms,
            DatasourceManager datasourceManager ) {
        this.linkingVertices = hazelcast.getMap( HazelcastMap.LINKING_VERTICES.name() );
        this.edges = hazelcast.getMap( HazelcastMap.EDGES.name() );
        this.listingService = listingService;
        this.dms = dms;
        this.datasourceManager = datasourceManager;
        this.hazelcast = hazelcast;
    }

    public void merge(
            UUID graphId,
            Set<UUID> ownablePropertyTypes,
            Set<UUID> propertyTypesToPopulate ) {
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets = new HashMap<>();

        // compute authorized property types for each of the linking entity sets, as well as the linked entity set
        // itself
        Set<UUID> linkingSets = listingService.getLinkedEntitySets( graphId );
        Iterable<UUID> involvedEntitySets = Iterables.concat( linkingSets, ImmutableSet.of( graphId ) );
        for ( UUID esId : involvedEntitySets ) {
            Set<UUID> propertiesOfEntitySet = dms.getEntityTypeByEntitySetId( esId ).getProperties();
            Set<UUID> authorizedProperties = Sets.intersection( ownablePropertyTypes, propertiesOfEntitySet );

            Map<UUID, PropertyType> authorizedPropertyTypes = authorizedProperties.stream()
                    .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );

            authorizedPropertyTypesForEntitySets.put( esId, authorizedPropertyTypes );
        }

        UUID syncId = datasourceManager.getCurrentSyncId( graphId );
        mergeVertices( graphId, syncId, authorizedPropertyTypesForEntitySets, propertyTypesToPopulate );
        mergeEdges( graphId, linkingSets, syncId );

    }

    private void mergeVertices(
            UUID graphId,
            UUID syncId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets,
            Set<UUID> propertyTypesToPopulate ) {
        logger.debug( "Linking: Merging vertices..." );

        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet = Maps.transformValues(
                authorizedPropertyTypesForEntitySets.get( graphId ), pt -> pt.getDatatype() );
        // EntityType.getAclKey returns an unmodifiable view of the underlying linked hash set, so the order is still
        // preserved, although

        Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet = Maps.newHashMap();
        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();

        authorizedPropertyTypesForEntitySets.entrySet().forEach( entry -> {
            propertyTypeIdsByEntitySet.put( entry.getKey(), entry.getValue().keySet() );
            propertyTypesById.putAll( entry.getValue() );
        } );

        int numVertices = linkingVertices
                .aggregate( new CountVerticesAggregator(), LinkingPredicates.graphId( graphId ) );
        ICountDownLatch latch = hazelcast.getCountDownLatch( graphId.toString() );
        latch.trySetCount( numVertices );

        linkingVertices.aggregate( new MergeVertexAggregator( graphId,
                syncId,
                propertyTypeIdsByEntitySet,
                propertyTypesById,
                propertyTypesToPopulate,
                authorizedPropertiesWithDataTypeForLinkedEntitySet ), LinkingPredicates.graphId( graphId ) );
    }

    private void mergeEdges( UUID linkedEntitySetId, Set<UUID> linkingSets, UUID syncId ) {
        logger.debug( "Linking: Merging edges..." );
        logger.debug( "Linking Sets: {}", linkingSets );
        UUID[] ids = linkingSets.toArray( new UUID[ 0 ] );

        Aggregator<Map.Entry<EdgeKey, LoomEdge>, Void> agg = new MergeEdgeAggregator( linkedEntitySetId, syncId );
        edges.aggregate( agg, Predicates.or( Predicates.in( PostgresEdgeMapstore.SRC_SET_ID, ids ),
                Predicates.in( PostgresEdgeMapstore.DST_SET_ID, ids ),
                Predicates.in( PostgresEdgeMapstore.EDGE_SET_ID, ids ) ) );
    }

}
