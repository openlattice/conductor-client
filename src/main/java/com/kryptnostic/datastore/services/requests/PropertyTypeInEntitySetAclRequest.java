package com.kryptnostic.datastore.services.requests;

import java.util.EnumSet;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.Principal;

public class PropertyTypeInEntitySetAclRequest extends EntitySetAclRequest {

    @JsonProperty( SerializationConstants.PROPERTY_FIELD )
    protected FullQualifiedName propertyTypeFqn;
    
    @JsonProperty( SerializationConstants.TIMESTAMP )
    protected String timestamp;

    public FullQualifiedName getPropertyType() {
        return propertyTypeFqn;
    }
    
    public String getTimestamp() {
        return timestamp;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setPrincipal( Principal principal ) {
        this.principal = principal;
        return this;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setAction( Action action ) {
        this.action = action;
        return this;
    }

    @Override
    public PropertyTypeInEntitySetAclRequest setPermissions( EnumSet<Permission> permissions ) {
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
    
    public PropertyTypeInEntitySetAclRequest setTimestamp( String timestamp ) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + ( propertyTypeFqn != null ? propertyTypeFqn.hashCode() : 0 );
        result = 31 * result + ( timestamp != null ? timestamp.hashCode() : 0 );
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

        if ( timestamp != null ? !timestamp.equals( that.timestamp ) : that.timestamp != null )
            return false;
        return propertyTypeFqn != null ? propertyTypeFqn.equals( that.propertyTypeFqn ) : that.propertyTypeFqn == null;
    }

    @Override
    public String toString() {
        return "PropertyTypeInEntitySetAclRequest [propertyTypeFqn=" + propertyTypeFqn + ", timestamp=" + timestamp
                + ", entitySetName=" + entitySetName + ", principal=" + principal + ", action=" + action
                + ", permissions=" + permissions + "]";
    }

    @JsonCreator
    public PropertyTypeInEntitySetAclRequest createRequest(
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.ACTION ) Action action,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String entitySetName,
            @JsonProperty( SerializationConstants.PROPERTY_FIELD ) FullQualifiedName propertyTypeFqn,
            @JsonProperty( SerializationConstants.PERMISSIONS ) EnumSet<Permission> permissions,
            @JsonProperty( SerializationConstants.TIMESTAMP ) String timestamp ) {
        return new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( action ).setName( entitySetName )
                .setPropertyType( propertyTypeFqn ).setPermissions( permissions ).setTimestamp( timestamp );
    }

}
