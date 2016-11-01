package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;

public class DerivePropertyTypeInEntitySetAclRequest {
    protected String                 role;
    protected FullQualifiedName      entityTypeFqn;
    protected String                 entitySetName;
    protected Set<FullQualifiedName> properties;

    @JsonCreator
    public DerivePropertyTypeInEntitySetAclRequest createAclRequest(
            @JsonProperty( SerializationConstants.ROLE ) String role,
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String entitySetName,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) Set<FullQualifiedName> properties ) {
        return new DerivePropertyTypeInEntitySetAclRequest().setRole( role ).setType( entityTypeFqn )
                .setName( entitySetName ).setProperties( properties );
    }

    public String getRole() {
        return role;
    }

    public FullQualifiedName getType() {
        return entityTypeFqn;
    }

    public String getName() {
        return entitySetName;
    }

    public Set<FullQualifiedName> getProperties() {
        return properties;
    }

    public DerivePropertyTypeInEntitySetAclRequest setRole( String role ) {
        this.role = role;
        return this;
    }

    public DerivePropertyTypeInEntitySetAclRequest setType( FullQualifiedName entityTypeFqn ) {
        this.entityTypeFqn = entityTypeFqn;
        return this;
    }

    public DerivePropertyTypeInEntitySetAclRequest setName( String entitySetName ) {
        this.entitySetName = entitySetName;
        return this;
    }

    public DerivePropertyTypeInEntitySetAclRequest setProperties( Set<FullQualifiedName> properties ) {
        this.properties = properties;
        return this;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        DerivePropertyTypeInEntitySetAclRequest that = (DerivePropertyTypeInEntitySetAclRequest) o;

        if ( role != null ? !role.equals( that.role ) : that.role != null )
            return false;
        if ( entityTypeFqn != null ? !entityTypeFqn.equals( that.entityTypeFqn ) : that.entityTypeFqn != null )
            return false;
        if ( entitySetName != null ? !entitySetName.equals( that.entitySetName ) : that.entitySetName != null )
            return false;
        return properties != null ? properties.equals( that.properties ) : that.properties == null;
    }

    @Override
    public int hashCode() {
        int result = role != null ? role.hashCode() : 0;
        result = 31 * result + ( entityTypeFqn != null ? entityTypeFqn.hashCode() : 0 );
        result = 31 * result + ( entitySetName != null ? entitySetName.hashCode() : 0 );
        result = 31 * result + ( properties != null ? properties.hashCode() : 0 );
        return result;
    }

}
