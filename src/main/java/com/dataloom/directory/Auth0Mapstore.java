package com.dataloom.directory;

import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.kryptnostic.datastore.services.Auth0ManagementApi;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotSupportedException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class Auth0Mapstore implements TestableSelfRegisteringMapStore<String, Auth0UserBasic> {
    private static final Logger logger = LoggerFactory.getLogger( Auth0Mapstore.class );
    private final Auth0ManagementApi auth0;
    private       HazelcastMap       map;

    public Auth0Mapstore( HazelcastMap map, Auth0ManagementApi auth0 ) {
        this.auth0 = checkNotNull( auth0, "Auth0 management api cannot be null" );
        this.map = checkNotNull( map, "Auth0 management api cannot be null" );
    }

    @Override
    public void store( String key, Auth0UserBasic value ) {
        auth0.resetRolesOfUser( key, ImmutableMap.of( "app_metadata", ImmutableMap.of( "roles", value.getRoles() ) ) );
    }

    @Override
    public void storeAll( Map<String, Auth0UserBasic> map ) {
        map.entrySet().stream().forEach( e -> store( e.getKey(), e.getValue() ) );
    }

    @Override
    public void delete( String key ) {
        throw new NotSupportedException( "Delete is not supported by this mapstore" );
    }

    @Override
    public void deleteAll( Collection<String> keys ) {
        throw new NotSupportedException( "Delete is not supported by this mapstore" );
    }

    @Override
    public Auth0UserBasic load( String key ) {
        return auth0.getUser( key );
    }

    @Override
    public Map<String, Auth0UserBasic> loadAll( Collection<String> keys ) {
        return Maps.asMap( ImmutableSet.copyOf( keys ), this::load );
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return getAllUsers().stream().map( Auth0UserBasic::getUserId )::iterator;
    }

    @Override
    public String getMapName() {
        return map.name();
    }

    @Override
    public String getTable() {
        return null;
    }

    @Override
    public MapStoreConfig getMapStoreConfig() {
        return new MapStoreConfig().setImplementation( this ).setEnabled( true )
                .setWriteDelaySeconds( 0 );
    }

    @Override
    public MapConfig getMapConfig() {
        return new MapConfig( getMapName() )
                .setBackupCount( 2 )
                .setMapStoreConfig( getMapStoreConfig() );
    }

    @Override
    public String generateTestKey() {
        return null;
    }

    @Override
    public Auth0UserBasic generateTestValue() {
        return null;
    }

    private Set<Auth0UserBasic> getAllUsers() {
        int page = 0;

        final Set<Auth0UserBasic> users = Sets.newHashSet();
        for (Set<Auth0UserBasic> pageOfUsers = auth0.getAllUsers( page, 100 );
              users.isEmpty() || pageOfUsers.size() == 100; pageOfUsers = auth0.getAllUsers( page++, 100 ) ) {
            users.addAll( pageOfUsers );
        }

        if ( users.isEmpty() ) {
            logger.warn( "Received null response from auth0" );
            return ImmutableSet.of();
        }

        return users;
    }


}
