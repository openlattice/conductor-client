package com.dataloom.matching;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.data.EntityKey;
import com.dataloom.data.aggregators.EntityAggregator;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.hazelcast.EntitySets;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.Entity;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingEntityKey;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.util.PersonMetric;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class MatchingAggregator extends Aggregator<Entry<EntityKey, UUID>, Double>
        implements HazelcastInstanceAware {
    private UUID                                graphId;
    private double[]                            lightest      = { Double.MAX_VALUE };
    private Map<UUID, UUID>                     entitySetIdsToSyncIds;
    private Map<UUID, PropertyType>             authorizedPropertyTypes;
    private Map<FullQualifiedName, String>      propertyTypeIdIndexedByFqn;
    private transient IMap<LinkingEdge, Double> weightedEdges = null;
    private transient IMap<DataKey, ByteBuffer> data          = null;
    private transient HazelcastLinkingGraphs    graphs        = null;
    private double[] numSingles = { 0 };

    private ConductorElasticsearchApi           elasticsearchApi;

    // Number of search results taken in each block.
    private int                                 blockSize     = 50;
    // Whether explanation for search results is stored.
    private boolean                             explain       = false;

    public MatchingAggregator(
            UUID graphId,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn ) {
        this(
                graphId,
                entitySetIdsToSyncIds,
                authorizedPropertyTypes,
                propertyTypeIdIndexedByFqn,
                new double[] { Double.MAX_VALUE },
                null );
    }

    public MatchingAggregator(
            UUID graphId,
            Map<UUID, UUID> entitySetIdsToSyncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            Map<FullQualifiedName, String> propertyTypesIndexedByFqn,
            double[] lightest,
            ConductorElasticsearchApi elasticsearchApi ) {
        this.graphId = graphId;
        this.entitySetIdsToSyncIds = entitySetIdsToSyncIds;
        this.lightest = lightest;
        this.authorizedPropertyTypes = authorizedPropertyTypes;
        this.propertyTypeIdIndexedByFqn = propertyTypesIndexedByFqn;
        this.elasticsearchApi = elasticsearchApi;
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        this.weightedEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
        this.data = hazelcastInstance.getMap( HazelcastMap.DATA.name() );
    }

    public void setElasticsearchApi( ConductorElasticsearchApi elasticsearchApi ) {
        this.elasticsearchApi = elasticsearchApi;
    }

    private Map<UUID, Set<String>> getEntity( EntityKey key ) {
        SetMultimap<UUID, ByteBuffer> byteBuffers = data.aggregate( new EntityAggregator(),
                EntitySets.getEntity(
                        key.getEntitySetId(),
                        key.getSyncId(),
                        key.getEntityId(),
                        authorizedPropertyTypes.keySet() ) )
                .getByteBuffers();
        SetMultimap<UUID, Object> entity = RowAdapters.entityIndexedById( key.getEntityId(),
                byteBuffers,
                authorizedPropertyTypes );
        return authorizedPropertyTypes.keySet().stream()
                .collect( Collectors.toMap( ptId -> ptId,
                        ptId -> entity.get( ptId ).stream()
                                .map( obj -> obj.toString() )
                                .collect( Collectors.toSet() ) ) );
    }

    @Override
    public void accumulate( Entry<EntityKey, UUID> input ) {
        EntityKey key = input.getKey();
        Map<UUID, Set<String>> entityWithProperties = getEntity( key );

        Map<String, Object> propertiesIndexedByString = entityWithProperties.entrySet().stream().collect(
                Collectors.toMap( entry -> entry.getKey().toString(), entry -> entry.getValue() ) );
        Entity currentEntity = new Entity( key, propertiesIndexedByString );

        // Blocking step: fire off query to elasticsearch.
        elasticsearchApi
                .executeEntitySetDataSearchAcrossIndices( entitySetIdsToSyncIds,
                        entityWithProperties,
                        blockSize,
                        explain )
                .stream().forEach( otherEntity -> {
                    if ( key.equals( otherEntity.getKey() ) ) {
                        graphs.getOrCreateVertex( graphId, key );
                        numSingles[0] += 1.0;
                    } else {
                        List<Entity> entityPair = Lists.newArrayList( currentEntity, otherEntity );
                        final LinkingEdge edge = fromEntities( graphId, entityPair );
                        double[] dist = PersonMetric.pDistance( currentEntity,
                                otherEntity,
                                propertyTypeIdIndexedByFqn );
                        double[][] features = new double[ 1 ][ 0 ];
                        features[ 0 ] = dist;
                        double weight = elasticsearchApi.getModelScore( features ) + 0.4;
                        lightest[ 0 ] = Math.min( lightest[ 0 ], weight );
                        graphs.setEdgeWeight( edge, weight );
                    }
                } );

    }

    private LinkingEdge fromEntities( UUID graphId, List<Entity> p ) {
        List<LinkingEntityKey> keys = p.stream()
                .map( e -> new LinkingEntityKey( graphId, e.getKey() ) ).collect( Collectors.toList() );
        LinkingVertexKey u = graphs.getOrCreateVertex( keys.get( 0 ) );
        LinkingVertexKey v = graphs.getOrCreateVertex( keys.get( 1 ) );
        return new LinkingEdge( u, v );
    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof MatchingAggregator ) {
            MatchingAggregator other = (MatchingAggregator) aggregator;
            if ( other.lightest[ 0 ] < lightest[ 0 ] ) lightest[ 0 ] = other.lightest[ 0 ];
            numSingles[0] += other.numSingles[0];
        }

    }

    @Override
    public Double aggregate() {
        return numSingles[0];
       // return lightest[ 0 ];
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

    public double[] getLightest() {
        return lightest;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( authorizedPropertyTypes == null ) ? 0 : authorizedPropertyTypes.hashCode() );
        result = prime * result + blockSize;
        result = prime * result + ( ( elasticsearchApi == null ) ? 0 : elasticsearchApi.hashCode() );
        result = prime * result + ( ( entitySetIdsToSyncIds == null ) ? 0 : entitySetIdsToSyncIds.hashCode() );
        result = prime * result + ( explain ? 1231 : 1237 );
        result = prime * result + ( ( graphId == null ) ? 0 : graphId.hashCode() );
        result = prime * result + Arrays.hashCode( lightest );
        result = prime * result
                + ( ( propertyTypeIdIndexedByFqn == null ) ? 0 : propertyTypeIdIndexedByFqn.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        MatchingAggregator other = (MatchingAggregator) obj;
        if ( authorizedPropertyTypes == null ) {
            if ( other.authorizedPropertyTypes != null ) return false;
        } else if ( !authorizedPropertyTypes.equals( other.authorizedPropertyTypes ) ) return false;
        if ( blockSize != other.blockSize ) return false;
        if ( entitySetIdsToSyncIds == null ) {
            if ( other.entitySetIdsToSyncIds != null ) return false;
        } else if ( !entitySetIdsToSyncIds.equals( other.entitySetIdsToSyncIds ) ) return false;
        if ( explain != other.explain ) return false;
        if ( graphId == null ) {
            if ( other.graphId != null ) return false;
        } else if ( !graphId.equals( other.graphId ) ) return false;
        if ( !Arrays.equals( lightest, other.lightest ) ) return false;
        if ( propertyTypeIdIndexedByFqn == null ) {
            if ( other.propertyTypeIdIndexedByFqn != null ) return false;
        } else if ( !propertyTypeIdIndexedByFqn.equals( other.propertyTypeIdIndexedByFqn ) ) return false;
        return true;
    }

}
