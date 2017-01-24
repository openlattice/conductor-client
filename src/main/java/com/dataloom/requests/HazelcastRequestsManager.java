package com.dataloom.requests;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastRequestsManager {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastRequestsManager.class );
    private final RequestQueryService                      rqs;
    private final IMap<AceKey, Status>                     requests;
    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;

    public HazelcastRequestsManager(
            HazelcastInstance hazelcastInstance,
            AuthorizationManager authorizations,
            RequestQueryService rqs ) {
        this.requests = hazelcastInstance.getMap( HazelcastMap.REQUESTS.name() );
        this.aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.rqs = checkNotNull( rqs );
    }

    public void submitAll( Map<AceKey, Status> statusMap ) {
        statusMap.entrySet().stream()
                .filter( e -> e.getValue().getStatus().equals( RequestStatus.APPROVED ) )
                .forEach( e -> aces.submitToKey( e.getKey(),
                        new PermissionMerger( DelegatedPermissionEnumSet.wrap( e.getValue().getPermissions() ) ) ) );
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
