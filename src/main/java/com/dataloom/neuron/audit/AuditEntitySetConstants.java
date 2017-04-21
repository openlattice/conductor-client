package com.dataloom.neuron.audit;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class AuditEntitySetConstants {

    // @formatter:off
    private AuditEntitySetConstants() {}
    // @formatter:on

    private static final String LOOM_AUDIT_NAMESPACE = "LOOM_AUDIT";

    private static final FullQualifiedName AUDIT_ET_FQN   = new FullQualifiedName( LOOM_AUDIT_NAMESPACE, "AUDIT" );
    private static final FullQualifiedName DETAILS_PT_FQN = new FullQualifiedName( LOOM_AUDIT_NAMESPACE, "DETAILS" );
    private static final FullQualifiedName TYPE_PT_FQN    = new FullQualifiedName( LOOM_AUDIT_NAMESPACE, "TYPE" );

    // TODO: where does this belong?
    public static final Principal LOOM_PRINCIPAL = new Principal( PrincipalType.USER, "Loom" );

    public static final PropertyType DETAILS_PT = new PropertyType(
            DETAILS_PT_FQN,
            "Details",
            Optional.of( "Any details about the event being logged." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String
    );

    public static final PropertyType TYPE_PT = new PropertyType(
            TYPE_PT_FQN,
            "Type",
            Optional.of( "The type of event being logged." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String
    );

    public static final EntityType AUDIT_ENTITY_TYPE = new EntityType(
            AUDIT_ET_FQN,
            "Loom Audit",
            "The Loom Audit Entity Type.",
            ImmutableSet.of(),
            Sets.newLinkedHashSet( Sets.newHashSet( TYPE_PT.getId() ) ),
            Sets.newLinkedHashSet( Sets.newHashSet( TYPE_PT.getId(), DETAILS_PT.getId() ) ),
            Optional.absent(),
            Optional.absent()
    );

    public static final EntitySet AUDIT_ENTITY_SET = new EntitySet(
            AUDIT_ENTITY_TYPE.getId(),
            "Loom Audit Entity Set",
            "Loom Audit Entity Set",
            Optional.of( "Loom Audit Entity Set" ),
            ImmutableSet.of( "info@thedataloom.com" )
    );

}
