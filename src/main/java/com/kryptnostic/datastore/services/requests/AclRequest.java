package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.Principal;

public class AclRequest {
    @JsonProperty( SerializationConstants.PRINCIPAL )
    protected Principal principal;
    @JsonProperty( SerializationConstants.ACTION )
    protected Action          action;
    @JsonProperty( SerializationConstants.PERMISSIONS )
    protected Set<Permission> permissions;

    public Principal getPrincipal() {
        return principal;
    }

    public Action getAction() {
        return action;
    }

    public Set<Permission> getPermissions() {
        return permissions;
    }

    public AclRequest setPrincipal( Principal principal ) {
        this.principal = principal;
        return this;
    }

    public AclRequest setAction( Action action ) {
        this.action = action;
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

        if ( principal != null ? !principal.equals( that.principal ) : that.principal != null )
            return false;
        if ( action != null ? !action.equals( that.action ) : that.action != null )
            return false;
        return permissions != null ? permissions.equals( that.permissions ) : that.permissions == null;
    }

    @Override
    public int hashCode() {
        int result = principal != null ? principal.hashCode() : 0;
        result = 31 * result + ( action != null ? action.hashCode() : 0 );
        result = 31 * result + ( permissions != null ? permissions.hashCode() : 0 );
        return result;
    }

    @JsonCreator
    public AclRequest createAclRequest(
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.ACTION ) Action action,
            @JsonProperty( SerializationConstants.PERMISSIONS ) Set<Permission> permissions ) {
        return new AclRequest().setPrincipal( principal ).setAction( action ).setPermissions( permissions );
    }

}
