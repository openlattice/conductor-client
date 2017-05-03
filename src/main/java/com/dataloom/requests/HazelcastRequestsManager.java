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
    private final RequestQueryService                      rqs;
    private final IMap<AceKey, Status>                     requests;
    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;

    private final Neuron neuron;

    public HazelcastRequestsManager(
            HazelcastInstance hazelcastInstance,
            RequestQueryService rqs,
            Neuron neuron ) {
        this.requests = hazelcastInstance.getMap( HazelcastMap.REQUESTS.name() );
        this.aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        this.rqs = checkNotNull( rqs );

        this.neuron = neuron;

        // TODO: it's not ideal to have to do "new SignalQueueReceptor( hazelcastInstance )"
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

    private void signalPermissionRequestStatusUpdates( Map<AceKey, Status> aceKeyRequestStatusMap ) {

        Set<AceKey> approved = Sets.newHashSet();
        Set<AceKey> declined = Sets.newHashSet();
        Set<AceKey> submitted = Sets.newHashSet();

        aceKeyRequestStatusMap.forEach( ( aceKey, requestStatus ) -> {

            List<UUID> newAclKey = Lists.newArrayList( aceKey.getKey() );
            if ( newAclKey.size() > 1 ) {
                newAclKey.remove( newAclKey.size() - 1 );
            }

            AceKey newAceKey = new AceKey( newAclKey, requestStatus.getPrincipal() );
            if ( requestStatus.getStatus().equals( RequestStatus.APPROVED ) ) {
                approved.add( newAceKey );
            } else if ( requestStatus.getStatus().equals( RequestStatus.DECLINED ) ) {
                declined.add( newAceKey );
            } else if ( requestStatus.getStatus().equals( RequestStatus.SUBMITTED ) ) {
                submitted.add( newAceKey );
            }
        } );

        approved.forEach( aceKey -> {
            this.neuron.transmit( new Signal(
                    SignalType.PERMISSION_REQUEST_APPROVED,
                    aceKey.getKey(),
                    aceKey.getPrincipal()
            ) );
        } );

        declined.forEach( aceKey -> {
            this.neuron.transmit( new Signal(
                    SignalType.PERMISSION_REQUEST_DECLINED,
                    aceKey.getKey(),
                    aceKey.getPrincipal()
            ) );
        } );

        submitted.forEach( aceKey -> {
            this.neuron.transmit( new Signal(
                    SignalType.PERMISSION_REQUEST_SUBMITTED,
                    aceKey.getKey(),
                    aceKey.getPrincipal()
            ) );
        } );
    }
}
