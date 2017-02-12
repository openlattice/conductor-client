package com.dataloom.graph;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;

/**
 * Implements a simple graph over a direct graph by imposing a canonical ordering on vertex order for edges.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastSimpleGraphService extends HazelcastGraphService {
    public HazelcastSimpleGraphService( HazelcastInstance hazelcastInstance ) {
        super( hazelcastInstance.getMap( HazelcastMap.EDGES.name() ) );
    }

    @Override
    public EntityKey getEdge( Edge edge ) {
        return super.getEdge( edge );
    }

    @Override
    public void addEdge( Edge edge, EntityKey value ) {
        super.addEdge( edge, value );
    }

    @Override
    public void removeEdge( Edge edge ) {
        super.removeEdge( edge );
    }
}
