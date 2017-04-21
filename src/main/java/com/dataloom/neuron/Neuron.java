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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.dataloom.neuron.audit.AuditLogQueryService;
import com.dataloom.neuron.signals.AuditableSignal;
import com.dataloom.neuron.signals.Signal;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;

import static com.dataloom.neuron.audit.AuditEntitySetConstants.AUDIT_ENTITY_SET;
import static com.dataloom.neuron.audit.AuditEntitySetConstants.AUDIT_ENTITY_TYPE;
import static com.dataloom.neuron.audit.AuditEntitySetConstants.DETAILS_PT;
import static com.dataloom.neuron.audit.AuditEntitySetConstants.LOOM_PRINCIPAL;
import static com.dataloom.neuron.audit.AuditEntitySetConstants.TYPE_PT;

public class Neuron {

    private static final Logger logger = LoggerFactory.getLogger( Neuron.class );

    private final AuditLogQueryService auditLogQueryService;
    private final DataGraphManager     dataGraphManager;
    private final DatasourceManager    dataSourceManager;
    private final EdmManager           entityDataModelManager;

    private final EnumMap<SignalType, Set<Receptor>> receptors = Maps.newEnumMap( SignalType.class );

    public Neuron(
            DataGraphManager dataGraphManager,
            DatasourceManager dataSourceManager,
            EdmManager entityDataModelManager,
            CassandraConfiguration cassandraConfig,
            Session session ) {

        this.auditLogQueryService = new AuditLogQueryService( cassandraConfig, session );
        this.dataGraphManager = dataGraphManager;
        this.dataSourceManager = dataSourceManager;
        this.entityDataModelManager = entityDataModelManager;

        ensureAuditEntitySetExists();
    }

    public void activateReceptor( SignalType type, Receptor receptor ) {

        if ( receptors.containsKey( type ) ) {
            receptors.get( type ).add( receptor );
        } else {
            receptors.put( type, Sets.newHashSet( receptor ) );
        }
    }

    public void transmit( Signal signal ) {

        // 1. write to the Audit Entity Set
        UUID auditId = writeToAuditEntitySet( signal );

        if ( auditId == null ) {
            return;
        }

        // 2. write to the Audit Log Table
        writeToAuditLog( signal, auditId );

        // 3. hand off event to receptors
        // List<Receptor> receptors = this.receptors.get( signal.getType() );
        // receptors.forEach( receptor -> receptor.process( signal ) );

    }

    private void ensureAuditEntitySetExists() {

        if ( entityDataModelManager.checkEntitySetExists( AUDIT_ENTITY_SET.getName() ) ) {
            return;
        }

        entityDataModelManager.createPropertyTypeIfNotExists( DETAILS_PT );
        entityDataModelManager.createPropertyTypeIfNotExists( TYPE_PT );
        entityDataModelManager.createEntityType( AUDIT_ENTITY_TYPE );
        entityDataModelManager.createEntitySet( LOOM_PRINCIPAL, AUDIT_ENTITY_SET );
    }

    private UUID writeToAuditEntitySet( Signal signal ) {

        UUID auditEntitySetId = AUDIT_ENTITY_SET.getId();
        UUID auditEntitySetSyncId = dataSourceManager.getCurrentSyncId( auditEntitySetId );

        // TODO: there has to be a better way to get "Map<UUID, EdmPrimitiveTypeKind> entityDataTypes"
        Map<UUID, EdmPrimitiveTypeKind> entityDataTypes = Maps.newHashMap();
        entityDataTypes.put( DETAILS_PT.getId(), DETAILS_PT.getDatatype() );
        entityDataTypes.put( TYPE_PT.getId(), TYPE_PT.getDatatype() );

        // TODO: there has to be a better way to get "SetMultimap<UUID, Object> entityProperties"
        SetMultimap<UUID, Object> entityProperties = HashMultimap.create();
        entityProperties.put( DETAILS_PT.getId(), signal.getDetails() );
        entityProperties.put( TYPE_PT.getId(), signal.getType() );

        // TODO: there has to be a better way to get Map<String, SetMultimap<UUID, Object>> entity
        Map<String, SetMultimap<UUID, Object>> entity = Maps.newHashMap();
        entity.put( AUDIT_ENTITY_SET.getName(), entityProperties );

        try {

            this.dataGraphManager.createEntities(
                    auditEntitySetId,
                    auditEntitySetSyncId,
                    entity,
                    entityDataTypes
            );

            // TODO: still need to get the audit ID
            return null;

        } catch ( ExecutionException | InterruptedException e ) {
            logger.error( e.getMessage(), e );
            return null;
        }
    }

    private void writeToAuditLog( Signal signal, UUID auditId ) {

        AuditableSignal auditableSignal = new AuditableSignal(
                signal.getType(),
                signal.getAclKey(),
                signal.getPrincipal(),
                signal.getDetails(),
                auditId,
                null,
                UUIDs.random(),
                UUIDs.timeBased()
        );

        this.auditLogQueryService.store( auditableSignal );
    }
}
