package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class PropertyTypeInEntityTypeAclRequest extends AclRequest {
    protected FullQualifiedName propertyTypeFqn;

    @Override
    public PropertyTypeInEntityTypeAclRequest setRole( String role ) {
        this.role = role;
        return this;
    }

    @Override
    public PropertyTypeInEntityTypeAclRequest setAction( Action action ) {
        this.action = action;
        return this;
    }

    @Override
    public PropertyTypeInEntityTypeAclRequest setFqn( FullQualifiedName fqn ) {
        this.fqn = fqn;
        return this;
    }

    @Override
    public PropertyTypeInEntityTypeAclRequest setPermissions( Set<Permission> permissions ) {
        this.permissions = permissions;
        return this;
    }
    
    public FullQualifiedName getPropertyType(){
        return propertyTypeFqn;
    }
    
    public PropertyTypeInEntityTypeAclRequest setPropertyType( FullQualifiedName propertyTypeFqn ){
        this.propertyTypeFqn = propertyTypeFqn;
        return this;
    }
    
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ( propertyTypeFqn != null ? propertyTypeFqn.hashCode() : 0 );
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
        
        PropertyTypeInEntityTypeAclRequest that = (PropertyTypeInEntityTypeAclRequest) obj;

        return propertyTypeFqn != null ? propertyTypeFqn.equals( that.propertyTypeFqn ) : that.propertyTypeFqn == null;
    }
    
    @JsonCreator
    public PropertyTypeInEntityTypeAclRequest createRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.ACTION) Action action,
            @JsonProperty( SerializationConstants.TYPE_FIELD) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD) FullQualifiedName propertyTypeFqn,
            @JsonProperty( SerializationConstants.PERMISSIONS) Set<Permission> permissions){
        return new PropertyTypeInEntityTypeAclRequest().setRole(role).setAction( action ).setFqn( entityTypeFqn ).setPropertyType( propertyTypeFqn ).setPermissions( permissions );
    }

}
