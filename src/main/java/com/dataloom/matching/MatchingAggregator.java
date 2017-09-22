package com.dataloom.matching;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.util.PersonMetric;
import com.google.common.collect.Lists;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;
import java.util.Map.Entry;

public class MatchingAggregator extends Aggregator<Entry<Set<EntityKey>, UUID>, Double>
        implements HazelcastInstanceAware {
    private UUID graphId;
    private double[] lightest = { Double.MAX_VALUE };
    private Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn;

    private transient IMap<GraphEntityPair, LinkingEntity> linkingEntities = null;
    private transient HazelcastLinkingGraphs         graphs          = null;

    private ConductorElasticsearchApi elasticsearchApi;

    public MatchingAggregator(
            UUID graphId,
            Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn ) {
        this(
                graphId,
                propertyTypeIdIndexedByFqn,
                new double[] { Double.MAX_VALUE },
                null );
    }

    public MatchingAggregator(
            UUID graphId,
            Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn,
            double[] lightest,
            ConductorElasticsearchApi elasticsearchApi ) {
        this.graphId = graphId;
        this.lightest = lightest;
        this.propertyTypeIdIndexedByFqn = propertyTypesIndexedByFqn;
        this.elasticsearchApi = elasticsearchApi;
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.graphs = new HazelcastLinkingGraphs( hazelcastInstance );
        this.linkingEntities = hazelcastInstance.getMap( HazelcastMap.LINKING_ENTITIES.name() );
    }

    @Override
    public void accumulate( Entry<Set<EntityKey>, UUID> input ) {
        List<EntityKey> keysAsList = Lists.newArrayList( input.getKey() );

        if ( keysAsList.size() == 1 ) {
            graphs.getOrCreateVertex( graphId, keysAsList.get( 0 ) );
        } else {
            EntityKey ek1 = keysAsList.get( 0 );
            EntityKey ek2 = keysAsList.get( 1 );
            LinkingVertexKey u = graphs.getOrCreateVertex( graphId, ek1 );
            LinkingVertexKey v = graphs.getOrCreateVertex( graphId, ek2 );
            final LinkingEdge edge = new LinkingEdge( u, v );

            Map<UUID, DelegatedStringSet> e1 = linkingEntities.get( new GraphEntityPair( graphId, ek1 ) ).getEntity();
            Map<UUID, DelegatedStringSet> e2 = linkingEntities.get( new GraphEntityPair( graphId, ek2 ) ).getEntity();

            double[] dist = PersonMetric.pDistance( e1, e2, propertyTypeIdIndexedByFqn );
            double[][] features = new double[ 1 ][ 0 ];
            features[ 0 ] = dist;
            double weight = elasticsearchApi.getModelScore( features ) + 0.4;
            lightest[ 0 ] = Math.min( lightest[ 0 ], weight );
            graphs.setEdgeWeight( edge, weight );
        }

    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof MatchingAggregator ) {
            MatchingAggregator other = (MatchingAggregator) aggregator;
            if ( other.lightest[ 0 ] < lightest[ 0 ] )
                lightest[ 0 ] = other.lightest[ 0 ];
        }

    }

    @Override
    public Double aggregate() {
        return lightest[ 0 ];
    }

    public UUID getGraphId() {
        return graphId;
    }

    public Map<FullQualifiedName, UUID> getPropertyTypeIdIndexedByFqn() {
        return propertyTypeIdIndexedByFqn;
    }

    public double[] getLightest() {
        return lightest;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        MatchingAggregator that = (MatchingAggregator) o;

        if ( graphId != null ? !graphId.equals( that.graphId ) : that.graphId != null )
            return false;
        if ( !Arrays.equals( lightest, that.lightest ) )
            return false;
        return propertyTypeIdIndexedByFqn != null ?
                propertyTypeIdIndexedByFqn.equals( that.propertyTypeIdIndexedByFqn ) :
                that.propertyTypeIdIndexedByFqn == null;
    }

    @Override public int hashCode() {
        int result = graphId != null ? graphId.hashCode() : 0;
        result = 31 * result + Arrays.hashCode( lightest );
        result = 31 * result + ( propertyTypeIdIndexedByFqn != null ? propertyTypeIdIndexedByFqn.hashCode() : 0 );
        return result;
    }
}
