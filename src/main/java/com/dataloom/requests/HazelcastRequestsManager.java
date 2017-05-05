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

package com.dataloom.requests;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AceKey;
import com.dataloom.authorization.DelegatedPermissionEnumSet;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.processors.PermissionMerger;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.neuron.Neuron;
import com.dataloom.neuron.SignalType;
import com.dataloom.neuron.receptors.SignalQueueReceptor;
import com.dataloom.neuron.signals.Signal;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class HazelcastRequestsManager {

    private static final Logger logger = LoggerFactory.getLogger( HazelcastRequestsManager.class );

    private final Neuron                                   neuron;
    private final RequestQueryService                      rqs;
    private final IMap<AceKey, Status>                     requests;
    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;

    private static final Map<RequestStatus, SignalType> STATE_TO_SIGNAL_MAP = Maps.newEnumMap( RequestStatus.class );

    static {
        STATE_TO_SIGNAL_MAP.put( RequestStatus.APPROVED, SignalType.PERMISSION_REQUEST_APPROVED );
        STATE_TO_SIGNAL_MAP.put( RequestStatus.DECLINED, SignalType.PERMISSION_REQUEST_DECLINED );
        STATE_TO_SIGNAL_MAP.put( RequestStatus.SUBMITTED, SignalType.PERMISSION_REQUEST_SUBMITTED );
    }

    public HazelcastRequestsManager( HazelcastInstance hazelcastInstance, RequestQueryService rqs, Neuron neuron ) {

        this.requests = hazelcastInstance.getMap( HazelcastMap.REQUESTS.name() );
        this.aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.rqs = checkNotNull( rqs );

        // TODO: it's not ideal to have to do "new SignalQueueReceptor( hazelcastInstance )"
        this.neuron = neuron;
        this.neuron.activateReceptor(
                EnumSet.of(
                        SignalType.PERMISSION_REQUEST_APPROVED,
                        SignalType.PERMISSION_REQUEST_DECLINED,
                        SignalType.PERMISSION_REQUEST_SUBMITTED
                ),
                new SignalQueueReceptor( hazelcastInstance )
        );
    }

    public void submitAll( Map<AceKey, Status> statusMap ) {

        statusMap
                .entrySet()
                .stream()
                .filter( e -> e.getValue().getStatus().equals( RequestStatus.APPROVED ) )
                .forEach( e -> aces.submitToKey(
                        e.getKey(), new PermissionMerger( e.getValue().getRequest().getPermissions() )
                ) );

        requests.putAll( statusMap );
        signalPermissionRequestStatusUpdates( statusMap );
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

    private void signalPermissionRequestStatusUpdates( Map<AceKey, Status> aceKeyRequestStateMap ) {

        /*
         * TODO: figure out a better approach to prepping this signal. since this is a very specific use case,
         * perhaps it makes sense to have a specific Signal or Receptor that handles this logic.
         */

        Map<RequestStatus, Set<AceKey>> stateToAceKeysMap = Maps.newEnumMap( RequestStatus.class );
        stateToAceKeysMap.put( RequestStatus.APPROVED, Sets.newHashSet() );
        stateToAceKeysMap.put( RequestStatus.DECLINED, Sets.newHashSet() );
        stateToAceKeysMap.put( RequestStatus.SUBMITTED, Sets.newHashSet() );

        aceKeyRequestStateMap.forEach( ( aceKey, requestStatus ) -> {

            List<UUID> newAclKey = Collections.unmodifiableList( aceKey.getKey() );
            if ( newAclKey.size() > 1 ) {
                // we need a new ArrayList, otherwise we get "java.io.NotSerializableException: java.util.ArrayList$SubList"
                newAclKey = Lists.newArrayList( newAclKey.subList( 0, newAclKey.size() - 1 ) );
            }

            stateToAceKeysMap
                    .get( requestStatus.getStatus() )
                    .add( new AceKey( newAclKey, requestStatus.getPrincipal() ) );
        } );

        stateToAceKeysMap.forEach( ( state, aceKeys ) -> {
            aceKeys.forEach( aceKey -> {
                this.neuron.transmit( new Signal(
                        STATE_TO_SIGNAL_MAP.get( state ),
                        aceKey.getKey(),
                        aceKey.getPrincipal()
                ) );
            } );
        } );
    }
}
