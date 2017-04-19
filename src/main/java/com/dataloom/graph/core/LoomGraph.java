package com.dataloom.graph.core;

import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.vertex.NeighborhoodSelection;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.hazelcast.core.HazelcastInstance;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoomGraph implements LoomGraphApi {

    private static final Logger logger = LoggerFactory.getLogger( LoomGraph.class );

    private final GraphQueryService     gqs;

    public LoomGraph( GraphQueryService gqs, HazelcastInstance hazelcastInstance ) {
        this.gqs = gqs;
    }

    @Override
    public void createVertex( UUID vertexId ) {
        createVertexAsync( vertexId ).getUninterruptibly();
    }

    @Override public ResultSetFuture createVertexAsync( UUID vertexId ) {
        return gqs.createVertexAsync( vertexId );
    }

    @Override
    public void addEdge(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {
        addEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                dstVertexId,
                dstVertexEntityTypeId,
                edgeEntityId,
                edgeEntityTypeId ).getUninterruptibly();
    }

    @Override
    public ResultSetFuture addEdgeAsync(
            UUID srcVertexId,
            UUID srcVertexEntityTypeId,
            UUID dstVertexId,
            UUID dstVertexEntityTypeId,
            UUID edgeEntityId,
            UUID edgeEntityTypeId ) {
        return gqs.putEdgeAsync( srcVertexId,
                srcVertexEntityTypeId,
                dstVertexId,
                dstVertexEntityTypeId,
                edgeEntityId,
                edgeEntityTypeId );
    }

    @Override
    public void deleteVertex( UUID vertexId ) {
        deleteVertexAsync( vertexId ).forEach( ResultSetFuture::getUninterruptibly );
    }

    @Override
    public Stream<ResultSetFuture> deleteVertexAsync( UUID vertex ) {
        NeighborhoodSelection ns = new NeighborhoodSelection( vertex, ImmutableSet.of(), ImmutableSet.of() );
        Stream<EdgeKey> edgesKey = gqs.getNeighborhood( ns );
        return edgesKey.map( this::deleteEdgeAsync );
    }

    @Override
    public LoomEdgeKey getEdge( EdgeKey key ) {
        return gqs.getEdge( key );
    }

    @Override
    public void deleteEdge( EdgeKey key ) {
        gqs.deleteEdge( key );
    }

    @Override
    public ResultSetFuture deleteEdgeAsync( EdgeKey edgeKey ) {
        return gqs.deleteEdgeAsync( edgeKey );
    }

    @Override
    public void deleteEdges( UUID srcId ) {
        gqs.deleteEdgesBySrcId( srcId );
    }

    @Override
    public Pair<List<LoomEdgeKey>, List<LoomEdgeKey>> getEdgesAndNeighborsForVertex( UUID vertexId ) {
        List<LoomEdgeKey> srcEdges = Lists.newArrayList( getEdges( new EdgeSelection(
                Optional.of( vertexId ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() ) ) );
        List<LoomEdgeKey> dstEdges = Lists.newArrayList( getEdges( new EdgeSelection(
                Optional.absent(),
                Optional.absent(),
                Optional.of( vertexId ),
                Optional.absent(),
                Optional.absent() ) ) );
        return Pair.of( srcEdges, dstEdges );
    }

}
