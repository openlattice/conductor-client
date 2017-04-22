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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

    public static PropertyType TYPE_PT;
    public static PropertyType DETAILS_PT;
    public static EntityType   AUDIT_ET;
    public static EntitySet    AUDIT_ES;

    private final DatasourceManager dataSourceManager;
    private final EdmManager        entityDataModelManager;

    public AuditEntitySet( DatasourceManager dataSourceManager, EdmManager entityDataModelManager ) {

        this.dataSourceManager = dataSourceManager;
        this.entityDataModelManager = entityDataModelManager;

        if ( entityDataModelManager.checkEntitySetExists( AUDIT_ENTITY_SET_NAME ) ) {
            initialize();
        } else {
            createAuditEntitySet();
        }
    }

    private void initialize() {

        TYPE_PT = entityDataModelManager.getPropertyType( TYPE_PT_FQN );
        DETAILS_PT = entityDataModelManager.getPropertyType( DETAILS_PT_FQN );
        AUDIT_ET = entityDataModelManager.getEntityType( AUDIT_ET_FQN );
        AUDIT_ES = entityDataModelManager.getEntitySet( AUDIT_ENTITY_SET_NAME );
    }

    private void createAuditEntitySet() {

        TYPE_PT = new PropertyType(
                TYPE_PT_FQN,
                "Type",
                Optional.of( "The type of event being logged." ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String
        );

        DETAILS_PT = new PropertyType(
                DETAILS_PT_FQN,
                "Details",
                Optional.of( "Any details about the event being logged." ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String
        );

        AUDIT_ET = new EntityType(
                AUDIT_ET_FQN,
                "Loom Audit",
                "The Loom Audit Entity Type.",
                ImmutableSet.of(),
                Sets.newLinkedHashSet( Sets.newHashSet( TYPE_PT.getId() ) ),
                Sets.newLinkedHashSet( Sets.newHashSet( TYPE_PT.getId(), DETAILS_PT.getId() ) ),
                Optional.absent(),
                Optional.absent()
        );

        AUDIT_ES = new EntitySet(
                AUDIT_ET.getId(),
                AUDIT_ENTITY_SET_NAME,
                AUDIT_ENTITY_SET_NAME,
                Optional.of( AUDIT_ENTITY_SET_NAME ),
                ImmutableSet.of( "info@thedataloom.com" )
        );

        entityDataModelManager.createPropertyTypeIfNotExists( TYPE_PT );
        entityDataModelManager.createPropertyTypeIfNotExists( DETAILS_PT );
        entityDataModelManager.createEntityType( AUDIT_ET );
        entityDataModelManager.createEntitySet( LOOM_PRINCIPAL, AUDIT_ES );

        UUID syncId = dataSourceManager.createNewSyncIdForEntitySet( AUDIT_ES.getId() );
        dataSourceManager.setCurrentSyncId( AUDIT_ES.getId(), syncId );
    }

    public static Collection<PropertyType> getProperties() {

        return ImmutableList.of(
                TYPE_PT,
                DETAILS_PT
        );
    }

    public static Map<UUID, EdmPrimitiveTypeKind> getPropertyDataTypesMap() {

        return getProperties()
                .stream()
                .collect(
                        Collectors.toMap( PropertyType::getId, PropertyType::getDatatype )
                );
    }
}
