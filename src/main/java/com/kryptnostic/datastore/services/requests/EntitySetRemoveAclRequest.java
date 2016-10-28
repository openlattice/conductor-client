package com.kryptnostic.datastore.services.requests;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;

public class EntitySetRemoveAclRequest {
    private FullQualifiedName type;
    private String name;
    
    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        EntitySetRemoveAclRequest that = (EntitySetRemoveAclRequest) o;

        if ( type != null ? !type.equals( that.type ) : that.type != null )
            return false;
        return name != null ? name.equals( that.name ) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + ( name != null ? name.hashCode() : 0 );
        return result;
    }
    
    @JsonCreator
    public static EntitySetRemoveAclRequest newEntitySet(
            @JsonProperty( SerializationConstants.TYPE_FIELD) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.NAME_FIELD) String entitySetName) {
        return new EntitySetRemoveAclRequest()
                .setType( entityTypeFqn )
                .setName( entitySetName );
    }

    public FullQualifiedName getType() {
        return type;
    }

    public EntitySetRemoveAclRequest setType( FullQualifiedName entityTypeFqn ) {
        this.type = entityTypeFqn;
        return this;
    }

    public String getName() {
        return name;
    }

    public EntitySetRemoveAclRequest setName( String entitySetName ) {
        this.name = entitySetName;
        return this;
    }
}
