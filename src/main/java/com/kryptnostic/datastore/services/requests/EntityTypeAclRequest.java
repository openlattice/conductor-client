package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class EntityTypeAclRequest {
    @JsonProperty( SerializationConstants.ROLE )
    protected String            role;
    @JsonProperty( SerializationConstants.ACTION )
    protected Action            action;
    @JsonProperty( SerializationConstants.TYPE_FIELD )
    protected FullQualifiedName entityTypeFqn;
    @JsonProperty( SerializationConstants.PERMISSIONS )
    protected Set<Permission>   permissions;

    public String getRole() {
        return role;
    }

    public Action getAction() {
        return action;
    }

    public FullQualifiedName getType() {
        return entityTypeFqn;
    }

    public Set<Permission> getPermissions() {
        return permissions;
    }

    public EntityTypeAclRequest setRole( String role ) {
        this.role = role;
        return this;
    }

    public EntityTypeAclRequest setAction( Action action ) {
        this.action = action;
        return this;
    }

    public EntityTypeAclRequest setType( FullQualifiedName entityTypeFqn ) {
        this.entityTypeFqn = entityTypeFqn;
        return this;
    }

    public EntityTypeAclRequest setPermissions( Set<Permission> permissions ) {
        this.permissions = permissions;
        return this;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        EntityTypeAclRequest that = (EntityTypeAclRequest) o;

        if ( role != null ? !role.equals( that.role ) : that.role != null )
            return false;
        if ( action != null ? !action.equals( that.action ) : that.action != null )
            return false;
        if ( entityTypeFqn != null ? !entityTypeFqn.equals( that.entityTypeFqn ) : that.entityTypeFqn != null )
            return false;
        return permissions != null ? permissions.equals( that.permissions ) : that.permissions == null;
    }

    @Override
    public int hashCode() {
        int result = role != null ? role.hashCode() : 0;
        result = 31 * result + ( action != null ? action.hashCode() : 0 );
        result = 31 * result + ( entityTypeFqn != null ? entityTypeFqn.hashCode() : 0 );
        result = 31 * result + ( permissions != null ? permissions.hashCode() : 0 );
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
