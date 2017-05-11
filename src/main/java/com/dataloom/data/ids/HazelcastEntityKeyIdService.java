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

package com.dataloom.data.ids;

import com.dataloom.data.EntityKey;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Most of the logic for this class is handled by the map store, which ensures a unique id
 * is assigned on read.
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastEntityKeyIdService implements EntityKeyIdService {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastEntityKeyIdService.class );
    private final ListeningExecutorService executor;

    private final IMap<EntityKey, UUID> ids;
    private final IMap<UUID, EntityKey> keys;

    public HazelcastEntityKeyIdService(
            HazelcastInstance hazelcastInstance,
            ListeningExecutorService executor ) {
        this.ids = hazelcastInstance.getMap( HazelcastMap.IDS.name() );
        this.keys = hazelcastInstance.getMap( HazelcastMap.KEYS.name() );
        this.executor = executor;
    }

    @Override
    public Optional<EntityKey> tryGetEntityKey( UUID entityKeyId ) {
        return Optional.ofNullable( getEntityKey( entityKeyId ) );
    }

    @Override
    public EntityKey getEntityKey( UUID entityKeyId ) {
        return Util.getSafely( keys, entityKeyId );
    }

    @Override public UUID getEntityKeyId( EntityKey entityKey ) {
        return Util.getSafely( ids, entityKey );
    }

    @Override 
    public Map<EntityKey, UUID> getEntityKeyIds( Set<EntityKey> entityKeys ) {
        return Util.getSafely( ids, entityKeys );
    }

    @Override
    public ListenableFuture<UUID> getEntityKeyIdAsync( EntityKey entityKey ) {
        return new ListenableHazelcastFuture<>( ids.getAsync( entityKey ) );
    }

    @Override
    public ListenableFuture<EntityKey> getEntityKeyAsync( UUID entityKeyId ) {
        return new ListenableHazelcastFuture<>( keys.getAsync( entityKeyId ) );
    }

}
