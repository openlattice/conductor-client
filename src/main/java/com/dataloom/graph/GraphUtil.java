package com.dataloom.graph;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.linking.util.UnorderedPair;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public final class GraphUtil {
    private static final Logger logger = LoggerFactory.getLogger( GraphUtil.class );

    private GraphUtil() {}

    public static EntityKey min( EntityKey a, EntityKey b ) {
        return a.compareTo( b ) < 0 ? a : b;
    }

    public static EntityKey max( EntityKey a, EntityKey b ) {
        return a.compareTo( b ) > 0 ? a : b;
    }

    public static LinkingEdge linkingEdge( UUID graphId, EntityKey a, EntityKey b ) {
        return new LinkingEdge( graphId, a, b );
    }

    public static LinkingEdge linkingEdge( UUID graphId, EntityKey... keys ) {
        if ( keys.length != 2 ) return null;
        return linkingEdge( graphId, keys[ 0 ], keys[ 1 ] );
    }

    public static LinkingEdge linkingEdge( UUID graphId, UnorderedPair<EntityKey> keys ) {
        return linkingEdge( graphId, keys.getAsArray() );
    }

    public static DirectedEdge edge( EntityKey a, EntityKey b ) {
        return new DirectedEdge( a, b );
    }

    public static boolean isNewEdge(
            HazelcastLinkingGraphs linkingGraph,
            UUID graphId,
            UnorderedPair<EntityKey> keys ) {
        if ( keys.getBackingCollection().size() <= 1 ) {
            return false;
        }
        LinkingEdge edge = linkingEdge( graphId, keys );
        return linkingGraph.getWeight( edge ) == null;
    }

}
