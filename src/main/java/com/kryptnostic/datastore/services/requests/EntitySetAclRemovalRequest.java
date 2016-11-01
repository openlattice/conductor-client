package com.kryptnostic.datastore.services.requests;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;

public class EntitySetAclRemovalRequest {

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    protected FullQualifiedName entityTypeFqn;
    @JsonProperty( SerializationConstants.NAME_FIELD )
    protected String            entitySetName;

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        EntitySetAclRemovalRequest that = (EntitySetAclRemovalRequest) o;

        if ( entityTypeFqn != null ? !entityTypeFqn.equals( that.entityTypeFqn ) : that.entityTypeFqn != null )
            return false;
        return entitySetName != null ? entitySetName.equals( that.entitySetName ) : that.entitySetName == null;
    }

    @Override
    public int hashCode() {
        int result = entityTypeFqn != null ? entityTypeFqn.hashCode() : 0;
        result = 31 * result + ( entitySetName != null ? entitySetName.hashCode() : 0 );
        return result;
    }

    public FullQualifiedName getType() {
        return entityTypeFqn;
    }

    public EntitySetAclRemovalRequest setType( FullQualifiedName entityTypeFqn ) {
        this.entityTypeFqn = entityTypeFqn;
        return this;
    }

    public String getName() {
        return entitySetName;
    }

    public EntitySetAclRemovalRequest setName( String entitySetName ) {
        this.entitySetName = entitySetName;
        return this;
    }

    @JsonCreator
    public static EntitySetAclRemovalRequest newEntitySet(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String entitySetName ) {
        return new EntitySetAclRemovalRequest()
                .setType( entityTypeFqn )
                .setName( entitySetName );
    }
}
