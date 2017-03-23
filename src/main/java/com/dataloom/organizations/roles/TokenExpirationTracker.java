package com.dataloom.organizations.roles;

import java.util.concurrent.TimeUnit;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;

/**
 * This class tracks users who needs to update their token by recording the last token acceptance time. It also exposes
 * helper method that decides whether token are accepted.
 * 
 * Warning: time used in this class must be in sync with Auth0 server time.
 * 
 * @author Ho Chung Siu
 *
 */
public class TokenExpirationTracker {
    private static final Logger logger                                  = LoggerFactory
            .getLogger( TokenExpirationTracker.class );

    private static final long   DEFAULT_AUTH0_JWT_TOKEN_EXPIRATION_TIME = 36000;                    // seconds

    private static final long   DEFAULT_ACCEPTANCE_TIME                 = getEpochSecond();         // server start time

    private static final String USERS_NEEDING_NEW_TOKEN                 = "users_needing_new_token";

    private IMap<String, Long>  tokenAcceptanceTime;
    private ISet<String>        usersNeedingNewToken;

    public TokenExpirationTracker( HazelcastInstance hazelcastInstance ) {
        this.tokenAcceptanceTime = hazelcastInstance.getMap( HazelcastMap.TOKEN_ACCEPTANCE_TIME.name() );
        this.usersNeedingNewToken = hazelcastInstance.getSet( USERS_NEEDING_NEW_TOKEN );
    }

    public void trackUser( String userId ) {
        tokenAcceptanceTime.put( userId, getEpochSecond(), DEFAULT_AUTH0_JWT_TOKEN_EXPIRATION_TIME, TimeUnit.SECONDS );
        usersNeedingNewToken.add( userId );
    }

    public void untrackUser( String userId ) {
        usersNeedingNewToken.remove( userId );
    }

    public boolean needsNewToken( String userId ) {
        return usersNeedingNewToken.contains( userId );
    }

    public boolean accept( String userId, Long tokenIssueTime ) {
        Long time = tokenAcceptanceTime.get( userId );
        return time == null ? ( DEFAULT_ACCEPTANCE_TIME <= tokenIssueTime ) : ( time <= tokenIssueTime );
    }

    public static long getEpochSecond() {
        // WARNING: This would give wrong results if Auth0 time and server time do not match.
        return Instant.now().getMillis() / 1000L;
    }
}