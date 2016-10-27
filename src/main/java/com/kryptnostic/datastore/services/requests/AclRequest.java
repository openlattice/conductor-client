package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class AclRequest {
    private final String role;
    private final Action action;
    private final FullQualifiedName fqn;
    private final Set<Permission> permissions;
    
    @JsonCreator
    public AclRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.ACTION) Action action,
            @JsonProperty( SerializationConstants.FQN) FullQualifiedName fqn,
            @JsonProperty( SerializationConstants.PERMISSIONS) Set<Permission> permissions){
        this.role = role;
        this.action = action;
        this.fqn = fqn;
        this.permissions = permissions;        
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

}
