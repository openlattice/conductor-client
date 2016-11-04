package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class PropertyTypeInEntitySetAclRequest extends EntitySetAclRequest {

    @JsonProperty( SerializationConstants.PROPERTY_FIELD )
    protected FullQualifiedName propertyTypeFqn;

    public FullQualifiedName getPropertyType() {
        return propertyTypeFqn;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setRole( String role ) {
        this.role = role;
        return this;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setAction( Action action ) {
        this.action = action;
        return this;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setPermissions( Set<Permission> permissions ) {
        this.permissions = permissions;
        return this;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setName( String entitySetName ) {
        this.entitySetName = entitySetName;
        return this;
    }

    public PropertyTypeInEntitySetAclRequest setPropertyType( FullQualifiedName propertyTypeFqn ) {
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
        if ( !super.equals( obj ) )
            return false;

        PropertyTypeInEntitySetAclRequest that = (PropertyTypeInEntitySetAclRequest) obj;

        return propertyTypeFqn != null ? propertyTypeFqn.equals( that.propertyTypeFqn ) : that.propertyTypeFqn == null;
    }

    @JsonCreator
    public PropertyTypeInEntitySetAclRequest createRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.ACTION ) Action action,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String entitySetName,
            @JsonProperty( SerializationConstants.PROPERTY_FIELD ) FullQualifiedName propertyTypeFqn,
            @JsonProperty( SerializationConstants.PERMISSIONS ) Set<Permission> permissions ) {
        return new PropertyTypeInEntitySetAclRequest().setRole( role ).setAction( action ).setName( entitySetName )
                .setPropertyType( propertyTypeFqn ).setPermissions( permissions );
    }

}
