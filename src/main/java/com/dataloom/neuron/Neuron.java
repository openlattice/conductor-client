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

package com.dataloom.neuron;

import java.util.EnumMap;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.auditing.AuditLogQueryService;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class Neuron {

    private static final Logger logger = LoggerFactory.getLogger( Neuron.class );

    private final AuditLogQueryService auditLogQueryService;

    private final EnumMap<SignalType, Set<Receptor>> receptors = Maps.newEnumMap( SignalType.class );

    public Neuron( AuditLogQueryService auditLogQueryService ) {

        this.auditLogQueryService = auditLogQueryService;
    }

    public void activateReceptor( SignalType type, Receptor receptor ) {

        if ( receptors.containsKey( type ) ) {
            receptors.get( type ).add( receptor );
        } else {
            receptors.put( type, Sets.newHashSet( receptor ) );
        }
    }

    public void transmit( AuditableSignal signal ) {

        // 1. audit event
        this.auditLogQueryService.store( signal );

        // 2. hand off event to receptors
        // List<Receptor> receptors = this.receptors.get( signal.getType() );
        // receptors.forEach( synapse -> synapse.process( signal ) );

    }
}
