package com.dataloom.organizations.roles;

import java.util.concurrent.TimeUnit;

import org.joda.time.Instant;

import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;

public class ExpiringTokenTracker {
    private static final long DEFAULT_AUTH0_JWT_TOKEN_EXPIRATION_TIME = 36000; // seconds
    //TODO may not be in sync with Auth0 time?
    private static final long DEFAULT_ACCEPTANCE_TIME = getEpochSecond(); // server start time
    
    private static final String USERS_NEEDING_NEW_TOKEN = "users_needing_new_token";
    
    private IMap<String, Long> tokenAcceptanceTime;
    private ISet<String> usersNeedingNewToken;
    
    public ExpiringTokenTracker( HazelcastInstance hazelcastInstance ){
        this.tokenAcceptanceTime = hazelcastInstance.getMap( HazelcastMap.TOKEN_ACCEPTANCE_TIME.name() );
        this.usersNeedingNewToken = hazelcastInstance.getSet( USERS_NEEDING_NEW_TOKEN );
    }
    
    public void trackUser( String userId ){
        tokenAcceptanceTime.put( userId, getEpochSecond(), DEFAULT_AUTH0_JWT_TOKEN_EXPIRATION_TIME, TimeUnit.SECONDS );
        usersNeedingNewToken.add( userId );
    }
    
    public void untrackUser( String userId ){
        usersNeedingNewToken.remove( userId );
    }
    
    public Long getAcceptanceTime( String userId ){
        Long time = tokenAcceptanceTime.get( userId );
        return time == null ? DEFAULT_ACCEPTANCE_TIME : time;
    }
    
    public boolean needsNewToken( String userId ){
        return usersNeedingNewToken.contains( userId );
    }
    
    public static long getEpochSecond(){
        return Instant.now().getMillis() / 1000L;
    }
}