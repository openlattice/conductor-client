package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class AclRequest {
    @JsonProperty( SerializationConstants.ROLE )
    protected String role;
    @JsonProperty( SerializationConstants.ACTION)
    protected Action action;
    @JsonProperty( SerializationConstants.FQN)
    protected FullQualifiedName fqn;
    @JsonProperty( SerializationConstants.PERMISSIONS)
    protected Set<Permission> permissions;
    
    @JsonCreator
    public AclRequest createAclRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.ACTION) Action action,
            @JsonProperty( SerializationConstants.FQN) FullQualifiedName fqn,
            @JsonProperty( SerializationConstants.PERMISSIONS) Set<Permission> permissions){
        return new AclRequest().setRole(role).setAction( action ).setFqn( fqn ).setPermissions( permissions );
    }
    
    public String getRole() {
        return role;
    }

    public Action getAction() {
        return action;
    }

    public FullQualifiedName getFqn() {
        return fqn;
    }

    public Set<Permission> getPermissions() {
        return permissions;
    }
    
    public AclRequest setRole( String role ) {
        this.role = role;
        return this;
    }

    public AclRequest setAction( Action action ) {
        this.action = action;
        return this;
    }

    public AclRequest setFqn( FullQualifiedName fqn ) {
        this.fqn = fqn;
        return this;
    }

    public AclRequest setPermissions( Set<Permission> permissions ) {
        this.permissions = permissions;
        return this;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        AclRequest that = (AclRequest) o;

        if ( role != null ? !role.equals( that.role ) : that.role != null )
            return false;
        if ( action != null ? !action.equals( that.action ) : that.action != null )
            return false;
        if ( fqn != null ? !fqn.equals( that.fqn ) : that.fqn != null )
            return false;
        return permissions != null ? permissions.equals( that.permissions ) : that.permissions == null;
    }

    @Override
    public int hashCode() {
        int result = role != null ? role.hashCode() : 0;
        result = 31 * result + ( action != null ? action.hashCode() : 0 );
        result = 31 * result + ( fqn != null ? fqn.hashCode() : 0 );
        result = 31 * result + ( permissions != null ? permissions.hashCode() : 0 );
        return result;
    }

}
