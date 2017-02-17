/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.linking;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.hazelcast.HazelcastUtils;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

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
    private final IMap<LinkingVertexKey, LinkingVertex> linkingVertices;
    private final IMap<LinkingEntityKey, UUID>          vertices;

    public HazelcastLinkingGraphs( HazelcastInstance hazelcastInstance ) {
        this.linkingEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
        this.linkingVertices = hazelcastInstance.getMap( HazelcastMap.LINKING_VERTICES.name() );
        this.vertices = hazelcastInstance.getMap( HazelcastMap.LINKING_ENTITY_VERTICES.name() );

    }

    public UUID getGraphIdFromEntitySetId( UUID linkedEntitySetId ){
        return linkedEntitySetId;
    }
    
    public LinkingVertexKey getOrCreateVertex( UUID graphId, EntityKey entityKey ) {
        LinkingEntityKey lek = new LinkingEntityKey( graphId, entityKey );
        UUID existingVertexId = vertices
                .putIfAbsent( lek, DEFAULT_ID );

        if ( existingVertexId != null ) {
            return new LinkingVertexKey( graphId, existingVertexId );
        }

        LinkingVertex vertex = new LinkingVertex( 0.0D, Sets.newHashSet( entityKey ) );

        LinkingVertexKey vertexKey = HazelcastUtils
                .insertIntoUnusedKey( linkingVertices,
                        vertex,
                        () -> new LinkingVertexKey( graphId, UUID.randomUUID() ) );
        vertices.set( lek, vertexKey.getVertexId() );
        return vertexKey;
    }

    public LinkingVertexKey merge( LinkingEdge edge ) {
        Double diameter = Util.getSafely( linkingEdges, edge );
        LinkingVertex u = Util.getSafely( linkingVertices, edge.getSrc() );
        LinkingVertex v = Util.getSafely( linkingVertices, edge.getDst() );
        Set<EntityKey> entityKeys = Sets
                .newHashSetWithExpectedSize( u.getEntityKeys().size() + v.getEntityKeys().size() );
        entityKeys.addAll( u.getEntityKeys() );
        entityKeys.addAll( v.getEntityKeys() );
        /*
         * As long as min edge is chosen for merging it is appropriate to use the edge weight as new diameter.
         */
        return HazelcastUtils.insertIntoUnusedKey( linkingVertices,
                new LinkingVertex( diameter.doubleValue(), entityKeys ),
                () -> new LinkingVertexKey( edge.getGraphId(), UUID.randomUUID() ) );
    }

    public void deleteVertex( LinkingVertexKey key ) {
        Util.deleteSafely( linkingVertices, key );
    }

    public void addEdge( LinkingEdge edge, double weight ) {
        linkingEdges.set( edge, weight );
    }

    public void removeEdge( LinkingEdge edge ) {
        linkingEdges.delete( edge );
    }

    public boolean edgeExists( LinkingEdge edge ) {
        return linkingEdges.containsKey( edge );
    }

    public LinkingVertexKey getLinkingVertextKey( LinkingEntityKey linkingEntityKey ) {
        return getOrCreateVertex( linkingEntityKey.getGraphId(), linkingEntityKey.getEntityKey() );
    }
}
