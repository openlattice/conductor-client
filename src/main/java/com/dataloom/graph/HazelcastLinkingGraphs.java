package com.dataloom.graph;

import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

/**
 * Implements a multiple simple graphs over by imposing a canonical ordering on vertex order for linkingEdges.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastLinkingGraphs {
    private final IMap<LinkingEdge, Double> linkingEdges;

    public HazelcastLinkingGraphs( HazelcastInstance hazelcastInstance ) {
        this.linkingEdges = hazelcastInstance.getMap( HazelcastMap.LINKING_EDGES.name() );
    }

    public double getWeight( LinkingEdge edge ) {
        return Util.getSafely( linkingEdges, edge );
    }

    public void addEdge( LinkingEdge edge, double weight ) {
        linkingEdges.set( edge, weight );
    }

    public void removeEdge( LinkingEdge edge ) {
        linkingEdges.delete( edge );
    }
}
