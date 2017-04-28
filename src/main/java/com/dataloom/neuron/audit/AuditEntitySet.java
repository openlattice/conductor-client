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

package com.dataloom.neuron.audit;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.data.DatasourceManager;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.neuron.signals.Signal;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.services.EdmManager;

/*
 * temporary solution for initializing the Audit EntitySet until we can figure out a better place for initializing
 * system-wide dependencies
 */
public class AuditEntitySet {

    private static final Logger logger = LoggerFactory.getLogger( AuditEntitySet.class );

    private static final String AUDIT_ENTITY_SET_NAME = "Loom Audit Entity Set";
    private static final String LOOM_AUDIT_NAMESPACE  = "LOOM_AUDIT";

    private static final FullQualifiedName DETAILS_PT_FQN = new FullQualifiedName( LOOM_AUDIT_NAMESPACE, "DETAILS" );
    private static final FullQualifiedName TYPE_PT_FQN    = new FullQualifiedName( LOOM_AUDIT_NAMESPACE, "TYPE" );
    private static final FullQualifiedName AUDIT_ET_FQN   = new FullQualifiedName( LOOM_AUDIT_NAMESPACE, "AUDIT" );

    // TODO: where does this belong?
    public static final Principal LOOM_PRINCIPAL = new Principal( PrincipalType.USER, "Loom" );

    private static Collection<PropertyType> PROPERTIES;
    private static PropertyType             TYPE_PROPERTY_TYPE;
    private static PropertyType             DETAILS_PROPERTY_TYPE;
    private static EntityType               AUDIT_ENTITY_TYPE;
    private static EntitySet                AUDIT_ENTITY_SET;

    private final DatasourceManager dataSourceManager;
    private final EdmManager        entityDataModelManager;

    public AuditEntitySet( DatasourceManager dataSourceManager, EdmManager entityDataModelManager ) {

        this.dataSourceManager = dataSourceManager;
        this.entityDataModelManager = entityDataModelManager;

        initializePropertyTypes();
        initializeEntityType();
        initializeEntitySet();
    }

    private void initializePropertyTypes() {

        try {
            TYPE_PROPERTY_TYPE = new PropertyType(
                    TYPE_PT_FQN,
                    "Type",
                    Optional.of( "The type of event being logged." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.String
            );
            entityDataModelManager.createPropertyTypeIfNotExists( TYPE_PROPERTY_TYPE );
        } catch ( Exception e ) {
            TYPE_PROPERTY_TYPE = entityDataModelManager.getPropertyType( TYPE_PT_FQN );
        }

        try {
            DETAILS_PROPERTY_TYPE = new PropertyType(
                    DETAILS_PT_FQN,
                    "Details",
                    Optional.of( "Any details about the event being logged." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.String
            );
            entityDataModelManager.createPropertyTypeIfNotExists( DETAILS_PROPERTY_TYPE );
        } catch ( Exception e ) {
            DETAILS_PROPERTY_TYPE = entityDataModelManager.getPropertyType( DETAILS_PT_FQN );
        }

        PROPERTIES = ImmutableList.of(
                TYPE_PROPERTY_TYPE,
                DETAILS_PROPERTY_TYPE
        );
    }

    private void initializeEntityType() {

        try {
            AUDIT_ENTITY_TYPE = new EntityType(
                    AUDIT_ET_FQN,
                    "Loom Audit",
                    "The Loom Audit Entity Type.",
                    ImmutableSet.of(),
                    Sets.newLinkedHashSet( Sets.newHashSet( TYPE_PROPERTY_TYPE.getId() ) ),
                    Sets.newLinkedHashSet( Sets
                            .newHashSet( TYPE_PROPERTY_TYPE.getId(), DETAILS_PROPERTY_TYPE.getId() ) ),
                    Optional.absent(),
                    Optional.absent()
            );
            entityDataModelManager.createEntityType( AUDIT_ENTITY_TYPE );
        } catch ( Exception e ) {
            AUDIT_ENTITY_TYPE = entityDataModelManager.getEntityType( AUDIT_ET_FQN );
        }
    }

    private void initializeEntitySet() {

        try {
            AUDIT_ENTITY_SET = new EntitySet(
                    AUDIT_ENTITY_TYPE.getId(),
                    AUDIT_ENTITY_SET_NAME,
                    AUDIT_ENTITY_SET_NAME,
                    Optional.of( AUDIT_ENTITY_SET_NAME ),
                    ImmutableSet.of( "info@thedataloom.com" )
            );
            entityDataModelManager.createEntitySet( LOOM_PRINCIPAL, AUDIT_ENTITY_SET );
        } catch ( Exception e ) {
            AUDIT_ENTITY_SET = entityDataModelManager.getEntitySet( AUDIT_ENTITY_SET_NAME );
        }

        UUID syncId = dataSourceManager.createNewSyncIdForEntitySet( AUDIT_ENTITY_SET.getId() );
        dataSourceManager.setCurrentSyncId( AUDIT_ENTITY_SET.getId(), syncId );
    }

    public static UUID getId() {

        return AUDIT_ENTITY_SET.getId();
    }

    public static Map<UUID, EdmPrimitiveTypeKind> getPropertyDataTypesMap() {

        return PROPERTIES
                .stream()
                .collect(
                        Collectors.toMap( PropertyType::getId, PropertyType::getDatatype )
                );
    }

    public static Map<String, SetMultimap<UUID, Object>> prepareAuditEntityData( Signal signal, String entityId ) {

        SetMultimap<UUID, Object> propertyValuesMap = HashMultimap.create();
        propertyValuesMap.put( DETAILS_PROPERTY_TYPE.getId(), signal.getDetails().or( "" ) );
        propertyValuesMap.put( TYPE_PROPERTY_TYPE.getId(), signal.getType().name() );

        Map<String, SetMultimap<UUID, Object>> auditEntityDataMap = Maps.newHashMap();
        auditEntityDataMap.put( entityId, propertyValuesMap );

        return auditEntityDataMap;
    }
}
