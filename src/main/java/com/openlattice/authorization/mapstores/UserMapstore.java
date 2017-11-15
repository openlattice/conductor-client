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

package com.openlattice.authorization.mapstores;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.kryptnostic.datastore.services.Auth0ManagementApi;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class UserMapstore implements TestableSelfRegisteringMapStore<String, Auth0UserBasic> {
    private static final Logger logger            = LoggerFactory.getLogger( UserMapstore.class );
    private static final int    DEFAULT_PAGE_SIZE = 1000;
    private static final int    TTL_SECONDS       = 15;
    //TODO: Switch over to a Hazelcast map to relieve pressure from Auth0
    private Retrofit                             retrofit;
    private Auth0ManagementApi                   auth0ManagementApi;
    private LoadingCache<String, Auth0UserBasic> auth0LoadingCache;

    public UserMapstore( String token ) {
        retrofit = RetrofitFactory.newClient( "https://openlattice.auth0.com/api/v2/", () -> token );

        auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );

        auth0LoadingCache = CacheBuilder.newBuilder()
                .expireAfterAccess( 15, TimeUnit.SECONDS )
                .build( new CacheLoader<String, Auth0UserBasic>() {
                    @Override public Auth0UserBasic load( String userId ) throws Exception {
                        return auth0ManagementApi.getUser( userId );
                    }
                } );
    }

    @Override public String getMapName() {
        return HazelcastMap.USERS.name();
    }

    @Override public String getTable() {
        //There is no table for auth0
        return null;
    }

    @Override public String generateTestKey() {
        return null;
    }

    @Override public Auth0UserBasic generateTestValue() {
        return null;
    }

    @Override public MapConfig getMapConfig() {
        return new MapConfig( getMapName() )
                .setTimeToLiveSeconds( TTL_SECONDS )
                .setMapStoreConfig( getMapStoreConfig() );
    }

    @Override public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig()
                .setImplementation( this )
                .setInitialLoadMode( InitialLoadMode.EAGER );
    }

    @Override public void store( String key, Auth0UserBasic value ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public void storeAll( Map<String, Auth0UserBasic> map ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public void delete( String key ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public void deleteAll( Collection<String> keys ) {
        throw new NotImplementedException( "Auth0 persistence not implemented." );
    }

    @Override public Auth0UserBasic load( String userId ) {
        return auth0LoadingCache.getUnchecked( userId );
    }

    @Override public Map<String, Auth0UserBasic> loadAll( Collection<String> keys ) {
        return keys.stream()
                .collect( Collectors.toMap( Function.identity(), this::load ) );
    }

    @Override public Iterable<String> loadAllKeys() {
        return () -> new Auth0UserIterator( auth0ManagementApi, auth0LoadingCache );
    }

    public static class Auth0UserIterator implements Iterator<String> {
        private final Auth0ManagementApi                   auth0ManagementApi;
        private final LoadingCache<String, Auth0UserBasic> auth0LoadingCache;
        private int page = 0;
        private Set<Auth0UserBasic> pageOfUsers;
        private Iterator<String>    pos;

        public Auth0UserIterator(
                Auth0ManagementApi auth0ManagementApi,
                LoadingCache<String, Auth0UserBasic> auth0LoadingCache ) {
            this.auth0ManagementApi = auth0ManagementApi;
            this.auth0LoadingCache = auth0LoadingCache;
            this.pos = ImmutableList.<String>of().iterator();
            next();
        }

        @Override
        public boolean hasNext() {
            //If iterator for current page is exhausted get the next page and refresh iterator.
            //Populate the loading cached to avoid repeated calls to auth0 for read user data
            if ( !pos.hasNext() ) {
                pageOfUsers = auth0ManagementApi.getAllUsers( page++, DEFAULT_PAGE_SIZE );
                if ( pageOfUsers.isEmpty() ) {
                    logger.warn( "Received null/empty response from auth0." );
                }
                auth0LoadingCache.putAll( pageOfUsers.stream()
                        .collect( Collectors.toMap( Auth0UserBasic::getUserId, Function.identity() ) ) );
                pos = pageOfUsers.stream().map( Auth0UserBasic::getUserId ).iterator();
            }
            return pos.hasNext();
        }

        @Override public String next() {
            return pos.next();
        }
    }
}
