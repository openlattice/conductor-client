package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class EntitySetAclRequest extends AclRequest {
    
    @JsonProperty( SerializationConstants.NAME_FIELD)
    protected String entitySetName;

    @Override
    public EntitySetAclRequest setRole( String role ) {
        this.role = role;
        return this;
    }

    @Override
    public EntitySetAclRequest setAction( Action action ) {
        this.action = action;
        return this;
    }

    @Override
    @JsonProperty( SerializationConstants.TYPE_FIELD)
    public EntitySetAclRequest setFqn( FullQualifiedName fqn ) {
        this.fqn = fqn;
        return this;
    }
    
    // TODO adhoc fix to force Jackson use type for this class - better solutions?
    @Override
    @JsonProperty( SerializationConstants.TYPE_FIELD)
    public FullQualifiedName getFqn( ) {
        return fqn;
    }
    
    @Override
    public EntitySetAclRequest setPermissions( Set<Permission> permissions ) {
        this.permissions = permissions;
        return this;
    }
    
    public String getEntitySetName(){
        return entitySetName;
    }
    
    public EntitySetAclRequest setEntitySetName( String entitySetName ){
        this.entitySetName = entitySetName;
        return this;
    }
    
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ( entitySetName != null ? entitySetName.hashCode() : 0 );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj )
            return true;
        if ( obj == null || getClass() != obj.getClass() )
            return false;
        if( !super.equals( obj ) )
            return false;
        
        EntitySetAclRequest that = (EntitySetAclRequest) obj;

        return entitySetName != null ? entitySetName.equals( that.entitySetName ) : that.entitySetName == null;
    }
    
    @JsonCreator
    public AclRequest createEntitySetAclRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.ACTION) Action action,
            @JsonProperty( SerializationConstants.TYPE_FIELD) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.NAME_FIELD) String entitySetName,
            @JsonProperty( SerializationConstants.PERMISSIONS) Set<Permission> permissions){
        return new EntitySetAclRequest().setRole(role).setAction( action ).setFqn( entityTypeFqn ).setEntitySetName( entitySetName ).setPermissions( permissions );
    }
}
