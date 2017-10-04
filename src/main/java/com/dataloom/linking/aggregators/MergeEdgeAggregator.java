/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.dataloom.linking.aggregators;

import com.dataloom.graph.core.LoomGraph;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.LinkingVertexKey;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MergeEdgeAggregator extends Aggregator<Entry<EdgeKey, LoomEdge>, Void> implements HazelcastInstanceAware {
    private static ListeningExecutorService executorService = MoreExecutors
            .listeningDecorator( Executors.newCachedThreadPool() );
    private final     UUID                         linkedEntitySetId;
    private final     UUID                         syncId;
    private transient IMap<LinkingVertexKey, UUID> newIds;
    private transient LoomGraph                    graph;

    public MergeEdgeAggregator( UUID linkedEntitySetId, UUID syncId ) {
        this.linkedEntitySetId = linkedEntitySetId;
        this.syncId = syncId;
    }

    @Override public void accumulate( Entry<EdgeKey, LoomEdge> input ) {
        mergeEdgeAsync( linkedEntitySetId, syncId, input.getValue() );
    }

    @Override public void combine( Aggregator aggregator ) {

    }

    @Override public Void aggregate() {
        return null;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.newIds = hazelcastInstance.getMap( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name() );
        graph = new LoomGraph( executorService, hazelcastInstance );
    }

    public UUID getMergedId( UUID graphId, UUID oldId ) {
        return Util.getSafely( newIds, new LinkingVertexKey( graphId, oldId ) );
    }

    private void mergeEdgeAsync( UUID linkedEntitySetId, UUID syncId, LoomEdge edge ) {
        UUID srcEntitySetId = edge.getSrcSetId();
        UUID srcSyncId = edge.getSrcSyncId();
        UUID dstEntitySetId = edge.getDstSetId();
        UUID dstSyncId = edge.getDstSyncId();
        UUID edgeEntitySetId = edge.getEdgeSetId();

        UUID srcId = edge.getKey().getSrcEntityKeyId();
        UUID dstId = edge.getKey().getDstEntityKeyId();
        UUID edgeId = edge.getKey().getEdgeEntityKeyId();

        UUID newSrcId = getMergedId( linkedEntitySetId, srcId );
        if ( newSrcId != null ) {
            srcEntitySetId = linkedEntitySetId;
            srcSyncId = syncId;
            srcId = newSrcId;
        }
        UUID newDstId = getMergedId( linkedEntitySetId, dstId );
        if ( newDstId != null ) {
            dstEntitySetId = linkedEntitySetId;
            dstSyncId = syncId;
            dstId = newDstId;
        }
        UUID newEdgeId = getMergedId( linkedEntitySetId, edgeId );
        if ( newEdgeId != null ) {
            edgeEntitySetId = linkedEntitySetId;
            edgeId = newEdgeId;
        }

        graph.addEdge( srcId,
                edge.getSrcTypeId(),
                srcEntitySetId,
                srcSyncId,
                dstId,
                edge.getDstTypeId(),
                dstEntitySetId,
                dstSyncId,
                edgeId,
                edge.getEdgeTypeId(),
                edgeEntitySetId );
    }

}
