package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class EntityTypeAclRequest extends AclRequest {

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    protected FullQualifiedName entityTypeFqn;

    public FullQualifiedName getType(){
        return entityTypeFqn;
    }
    
    @Override
    public EntityTypeAclRequest setRole( String role ) {
        this.role = role;
        return this;
    }

    @Override
    public EntityTypeAclRequest setAction( Action action ) {
        this.action = action;
        return this;
    }

    @Override
    public EntityTypeAclRequest setPermissions( Set<Permission> permissions ) {
        this.permissions = permissions;
        return this;
    }

    public EntityTypeAclRequest setType( FullQualifiedName entityTypeFqn ) {
        this.entityTypeFqn = entityTypeFqn;
        return this;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        if ( !super.equals( o ) )
            return false;
        
        EntityTypeAclRequest that = (EntityTypeAclRequest) o;

        return entityTypeFqn != null ? entityTypeFqn.equals( that.entityTypeFqn ) : that.entityTypeFqn == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ( entityTypeFqn != null ? entityTypeFqn.hashCode() : 0 );
        return result;
    }

    @JsonCreator
    public EntityTypeAclRequest createAclRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.ACTION ) Action action,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.PERMISSIONS ) Set<Permission> permissions ) {
        return new EntityTypeAclRequest().setRole( role ).setAction( action ).setType( entityTypeFqn )
                .setPermissions( permissions );
    }

}
