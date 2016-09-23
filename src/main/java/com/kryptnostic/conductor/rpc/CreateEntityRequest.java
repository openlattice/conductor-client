package com.kryptnostic.conductor.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;
import java.util.UUID;

/**
 * Created by yao on 9/20/16.
 */
public class CreateEntityRequest {
    private final Optional<UUID>                               aclId;
    private final Optional<UUID>                               syncId;
    private final String                                       entitySetName;
    private final FullQualifiedName                            entityType;
    private final Set<HashMultimap<FullQualifiedName, Object>> propertyValues;

    @JsonCreator
    public CreateEntityRequest(

            @JsonProperty( SerializationConstants.ENTITY_SET_NAME ) String entitySetName,
            @JsonProperty( SerializationConstants.KEY_FIELD ) FullQualifiedName entityType,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
                    Set<HashMultimap<FullQualifiedName, Object>> propertyValues,
            @JsonProperty( SerializationConstants.ACL_ID_FIELD ) Optional<UUID> aclId,
            @JsonProperty( SerializationConstants.SYNC_ID ) Optional<UUID> syncId ) {
        this.aclId = aclId;
        this.syncId = syncId;
        this.entitySetName = entitySetName;

        this.entityType = entityType;
        this.propertyValues = propertyValues;
    }

    @JsonProperty( SerializationConstants.ACL_ID_FIELD )
    public Optional<UUID> getAclId() {

        return aclId;
    }

    @JsonProperty( SerializationConstants.SYNC_ID )
    public Optional<UUID> getSyncId() {
        return syncId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_NAME )
    public String getEntitySetName() {
        return entitySetName;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public FullQualifiedName getEntityType() {
        return entityType;
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public Set<HashMultimap<FullQualifiedName, Object>> getPropertyValues() {
        return propertyValues;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        CreateEntityRequest that = (CreateEntityRequest) o;

        if ( aclId != null ? !aclId.equals( that.aclId ) : that.aclId != null )
            return false;
        if ( syncId != null ? !syncId.equals( that.syncId ) : that.syncId != null )
            return false;
        if ( entitySetName != null ? !entitySetName.equals( that.entitySetName ) : that.entitySetName != null )
            return false;
        if ( entityType != null ? !entityType.equals( that.entityType ) : that.entityType != null )
            return false;
        return propertyValues != null ? propertyValues.equals( that.propertyValues ) : that.propertyValues == null;

    }

    @Override public int hashCode() {
        int result = aclId != null ? aclId.hashCode() : 0;
        result = 31 * result + ( syncId != null ? syncId.hashCode() : 0 );
        result = 31 * result + ( entitySetName != null ? entitySetName.hashCode() : 0 );
        result = 31 * result + ( entityType != null ? entityType.hashCode() : 0 );
        result = 31 * result + ( propertyValues != null ? propertyValues.hashCode() : 0 );
        return result;
    }
}
