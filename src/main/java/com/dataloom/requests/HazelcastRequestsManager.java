package com.dataloom.requests;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.Principal;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastRequestsManager {
    private static final Logger        logger = LoggerFactory.getLogger( HazelcastRequestsManager.class );
    private final RequestQueryService  rqs;
    private final IMap<AceKey, Status> requests;

    public HazelcastRequestsManager(
            HazelcastInstance hazelcastInstance,
            RequestQueryService rqs ) {
        this.requests = hazelcastInstance.getMap( HazelcastMap.REQUESTS.name() );
        this.rqs = checkNotNull( rqs );
    }

    public void submitAll( Map<AceKey, Status> statusMap ) {
        requests.putAll( statusMap );
    }

    public Stream<Status> getStatuses( Principal principal ) {
        return getStatuses( rqs.getRequestKeys( principal ) );
    }

    public Stream<Status> getStatuses( Principal principal, RequestStatus requestStatus ) {
        return getStatuses( rqs.getRequestKeys( principal, requestStatus ) );
    }

    public Stream<Status> getStatusesForAllUser( List<UUID> aclKey ) {
        return getStatuses( rqs.getRequestKeys( aclKey ) );
    }

    public Stream<Status> getStatusesForAllUser( List<UUID> aclKey, RequestStatus requestStatus ) {
        return getStatuses( rqs.getRequestKeys( aclKey, requestStatus ) );
    }

    public Stream<Status> getStatuses( Stream<AceKey> requestKeys ) {
        return requestKeys.map( Util.getSafeMapper( requests ) );
    }
}
