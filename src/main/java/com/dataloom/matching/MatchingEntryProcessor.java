package com.dataloom.matching;

import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.hazelcast.Entities;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.*;
import com.dataloom.linking.util.PersonMetric;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorServiceFuture;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class MatchingEntryProcessor
        extends AbstractRhizomeEntryProcessor<EntityKey, UUID, Double> implements HazelcastInstanceAware {
    private UUID                           graphId;
    private Map<UUID, UUID>                entitySetIdsToSyncIds;
    private Map<UUID, PropertyType>        authorizedPropertyTypes;
    private Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn;

    private transient IMap<LinkingEdge, Double> weightedEdges = null;
    private transient IMap<DataKey, ByteBuffer> data          = null;
    private transient HazelcastLinkingGraphs    graphs        = null;
    private Entities entities;
    private double count = 0;
    private double time  = 0;
    private transient DurableExecutorService executor;

    private ConductorElasticsearchApi elasticsearchApi;

    // Number of search results taken in each block.
    private int     blockSize = 50;
    // Whether explanation for search results is stored.
    private boolean explain   = false;

    public MatchingEntryProcessor(
            UUID graphId,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn,
            Entities entities ) {
        this(
                graphId,
                entitySetIdsToSyncIds,
                authorizedPropertyTypes,
                propertyTypeIdIndexedByFqn,
                entities,
                null );
    }

    public MatchingEntryProcessor(
            UUID graphId,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            Map<FullQualifiedName, String> propertyTypesIndexedByFqn,
            Entities entities,
            ConductorElasticsearchApi elasticsearchApi ) {
        this.graphId = graphId;
        this.entitySetIdsToSyncIds = entitySetIdsToSyncIds;
        this.authorizedPropertyTypes = authorizedPropertyTypes;
        this.entities = entities;
        this.propertyTypeIdIndexedByFqn = propertyTypesIndexedByFqn;
        this.elasticsearchApi = elasticsearchApi;
    }

    @Override
    public Double process( Map.Entry<EntityKey, UUID> entry ) {
        EntityKey key = entry.getKey();
        UUID entityKeyId = entry.getValue();
        Map<UUID, Set<String>> entityWithProperties = getEntity( key, entityKeyId );

        Map<String, Object> propertiesIndexedByString = entityWithProperties.entrySet().stream().collect(
                Collectors.toMap( mapEntry -> mapEntry.getKey().toString(), mapEntry -> mapEntry.getValue() ) );
        Entity currentEntity = new Entity( key, propertiesIndexedByString );
//
//        List<Entity> searchResults = elasticsearchApi
//                .executeEntitySetDataSearchAcrossIndices( entitySetIdsToSyncIds,
//                        entityWithProperties,
//                        blockSize,
//                        explain );
//        double[] lightest = { Double.MAX_VALUE };
//
//        // Blocking step: fire off query to elasticsearch.
//        searchResults.stream().forEach( otherEntity -> {
//            if ( key.equals( otherEntity.getKey() ) ) {
//                graphs.getOrCreateVertex( graphId, key );
//            } else {
//                List<Entity> entityPair = Lists.newArrayList( currentEntity, otherEntity );
//                final LinkingEdge edge = fromEntities( graphId, entityPair );
//                double[] dist = PersonMetric.pDistance( currentEntity,
//                        otherEntity,
//                        propertyTypeIdIndexedByFqn );
//                double[][] features = new double[ 1 ][ 0 ];
//                features[ 0 ] = dist;
//                double weight = elasticsearchApi.getModelScore( features ) + 0.4;
//                lightest[ 0 ] = Math.min( lightest[ 0 ], weight );
//                graphs.setEdgeWeight( edge, weight );
//            }
//        } );
        return 1.0;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        this.weightedEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
        this.data = hazelcastInstance.getMap( HazelcastMap.DATA.name() );
        this.executor = hazelcastInstance.getDurableExecutorService( "default" );
    }

    public void setElasticsearchApi( ConductorElasticsearchApi elasticsearchApi ) {
        this.elasticsearchApi = elasticsearchApi;
    }

    private Map<UUID, Set<String>> getEntity( EntityKey key, UUID entityKeyId ) {
        SetMultimap<UUID, ByteBuffer> byteBuffers = entities.get( entityKeyId );
        SetMultimap<UUID, Object> entity = RowAdapters.entityIndexedById( key.getEntityId(),
                byteBuffers,
                authorizedPropertyTypes );
        return authorizedPropertyTypes.keySet().stream()
                .collect( Collectors.toMap( ptId -> ptId,
                        ptId -> entity.get( ptId ).stream()
                                .map( obj -> obj.toString() )
                                .collect( Collectors.toSet() ) ) );
    }

    private LinkingEdge fromEntities( UUID graphId, List<Entity> p ) {
        List<LinkingEntityKey> keys = p.stream()
                .map( e -> new LinkingEntityKey( graphId, e.getKey() ) ).collect( Collectors.toList() );
        LinkingVertexKey u = graphs.getOrCreateVertex( keys.get( 0 ) );
        LinkingVertexKey v = graphs.getOrCreateVertex( keys.get( 1 ) );
        return new LinkingEdge( u, v );
    }

    public UUID getGraphId() {
        return graphId;
    }

    public Map<UUID, UUID> getEntitySetIdsToSyncIds() {
        return entitySetIdsToSyncIds;
    }

    public Map<UUID, PropertyType> getAuthorizedPropertyTypes() {
        return authorizedPropertyTypes;
    }

    public Map<FullQualifiedName, String> getPropertyTypeIdIndexedByFqn() {
        return propertyTypeIdIndexedByFqn;
    }

    public Entities getEntities() {
        return entities;
    }
}
