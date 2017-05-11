package com.dataloom.linking;

import java.util.UUID;

import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

public class HazelcastVertexMergingService {
    private IMap<LinkingVertexKey, UUID> newIds;

    public HazelcastVertexMergingService( HazelcastInstance hazelcastInstance ) {
        this.newIds = hazelcastInstance.getMap( HazelcastMap.VERTEX_IDS_AFTER_LINKING.name() );
    }

    public void saveToLookup( UUID graphId, UUID oldId, UUID newId ){
        newIds.put( new LinkingVertexKey( graphId, oldId ), newId );
    }
    
    public UUID getMergedId( UUID graphId, UUID oldId ){
        return Util.getSafely( newIds, new LinkingVertexKey( graphId, oldId ) );
    }
}
