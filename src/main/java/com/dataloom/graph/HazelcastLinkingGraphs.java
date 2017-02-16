package com.dataloom.graph;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.mapstores.LinkingVertexKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.HazelcastUtils;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Implements a multiple simple graphs over by imposing a canonical ordering on vertex order for linkingEdges.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastLinkingGraphs {
    private static final UUID DEFAULT_ID = new UUID( 0, 0 );
    private final IMap<LinkingEdge, Double>             linkingEdges;
    private final IMap<LinkingVertexKey, LinkingVertex> vertexWeights;
    private final IMap<LinkingEntityKey, UUID>          vertices;

    public HazelcastLinkingGraphs( HazelcastInstance hazelcastInstance ) {
        this.linkingEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
    }

    public LinkingVertexKey createVertex( UUID graphId, EntityKey entityKey ) {
        LinkingEntityKey lek = new LinkingEntityKey( graphId, entityKey );
        UUID existingVertexId = vertices
                .putIfAbsent( lek, DEFAULT_ID );

        if ( existingVertexId != null ) {
            return new LinkingVertexKey( graphId, existingVertexId );
        }

        LinkingVertex vertex = new LinkingVertex( 0.0D, Sets.newHashSet( entityKey ) );

        LinkingVertexKey vertexKey = HazelcastUtils
                .insertIntoUnusedKey( vertexWeights, vertex, () -> new LinkingVertexKey( graphId, UUID.randomUUID() ) );
        vertices.set( lek, vertexKey.getVertexId() );
        return vertexKey;
    }

    public LinkingVertexKey merge( LinkingEdge edge ) {
        Double diameter = Util.getSafely( linkingEdges, edge );
        LinkingVertex u = Util.getSafely( vertexWeights, edge.getSrc() );
        LinkingVertex v = Util.getSafely( vertexWeights, edge.getDst() );
        Set<EntityKey> entityKeys = Sets
                .newHashSetWithExpectedSize( u.getEntityKeys().size() + v.getEntityKeys().size() );
        entityKeys.addAll( u.getEntityKeys() );
        entityKeys.addAll( v.getEntityKeys() );
        /*
         * As long as min edge is chosen for merging it is appropriate to use the edge weight as new diameter.
         */
        LinkingVertexKey newVertexKey = HazelcastUtils.insertIntoUnusedKey( vertexWeights,
                new LinkingVertex( diameter.doubleValue(), entityKeys ),
                () -> new LinkingVertexKey( edge.getGraphId(), UUID.randomUUID() ) );

    }

    public void deleteVertex( LinkingVertexKey key ) {
        Util.deleteSafely( vertexWeights, key );
    }

    public LinkingVertexKey getVertexKey( EntityKey key ) {
        return Util.getSafely( vertices, key );
    }

    public Double getVertexWeight( LinkingVertexKey vertex ) {
        return Util.getSafely( vertexWeights, vertex );
    }

    public Double getEdgeWeight( LinkingEdge edge ) {
        return Util.getSafely( linkingEdges, edge );
    }

    public void addEdge( LinkingEdge edge, double weight ) {
        linkingEdges.set( edge, weight );
    }

    public void removeEdge( LinkingEdge edge ) {
        linkingEdges.delete( edge );
    }

    public Map<LinkingEdge, Double> getAllWeights( Set<LinkingEdge> neighboringEdges ) {
        return null;
    }
}
