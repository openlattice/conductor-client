package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;

public class PropertyTypeInEntitySetAclRequest extends AclRequest {

    @JsonProperty( SerializationConstants.NAME_FIELD )
    protected String            entitySetName;
    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    protected FullQualifiedName propertyTypeFqn;

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
    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public PropertyTypeInEntitySetAclRequest setFqn( FullQualifiedName fqn ) {
        this.fqn = fqn;
        return this;
    }

    @Override
    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public FullQualifiedName getFqn() {
        return fqn;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setPermissions( Set<Permission> permissions ) {
        this.permissions = permissions;
        return this;
    }

    public String getName() {
        return entitySetName;
    }

    public PropertyTypeInEntitySetAclRequest setName( String entitySetName ) {
        this.entitySetName = entitySetName;
        return this;
    }

    public FullQualifiedName getPropertyType() {
        return propertyTypeFqn;
    }

    public PropertyTypeInEntitySetAclRequest setPropertyType( FullQualifiedName propertyTypeFqn ) {
        this.propertyTypeFqn = propertyTypeFqn;
        return this;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ( entitySetName != null ? entitySetName.hashCode() : 0 );
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

        if ( entitySetName != null ? !entitySetName.equals( that.entitySetName ) : that.entitySetName != null )
            return false;

        return propertyTypeFqn != null ? propertyTypeFqn.equals( that.propertyTypeFqn ) : that.propertyTypeFqn == null;
    }

    @JsonCreator
    public PropertyTypeInEntitySetAclRequest createRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.ACTION ) Action action,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String entitySetName,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) FullQualifiedName propertyTypeFqn,
            @JsonProperty( SerializationConstants.PERMISSIONS ) Set<Permission> permissions ) {
        return new PropertyTypeInEntitySetAclRequest().setRole( role ).setAction( action ).setFqn( entityTypeFqn )
                .setName( entitySetName ).setPropertyType( propertyTypeFqn ).setPermissions( permissions );
    }

}
