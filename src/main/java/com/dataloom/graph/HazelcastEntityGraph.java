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

package com.dataloom.graph;

import com.dataloom.data.EntityKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public abstract class HazelcastEntityGraph {
    private final IMap<DirectedEdge, EntityKey> edges;
    //    private final IMap<EntityKey, SetMultimap<UUID,Object>> entities;

    public HazelcastEntityGraph( HazelcastInstance hazelcastInstance) {
        this.edges = hazelcastInstance.getMap( HazelcastMap.EDGES.name() );
    }

    public EntityKey getEdge( DirectedEdge edge ) {
        return edges.get( edge );
    }

    public void createOrUpdateEdge( DirectedEdge edge, EntityKey vertex ) {
        edges.set( edge, vertex );
    }

    public void deleteEdge( DirectedEdge edge ) {
        edges.delete( edge );
    }

}
