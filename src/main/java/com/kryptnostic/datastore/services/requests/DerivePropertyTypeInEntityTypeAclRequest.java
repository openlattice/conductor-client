package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Principal;

public class DerivePropertyTypeInEntityTypeAclRequest {
    protected Principal principal;
    protected FullQualifiedName entityTypeFqn;
    protected Set<FullQualifiedName> properties;
    
    @JsonCreator
    public DerivePropertyTypeInEntityTypeAclRequest createAclRequest(
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.TYPE_FIELD) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD) Set<FullQualifiedName> properties){
        return new DerivePropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setType( entityTypeFqn ).setProperties( properties );
    }
    
    public Principal getPrincipal() {
        return principal;
    }

    public FullQualifiedName getType() {
        return entityTypeFqn;
    }

    public Set<FullQualifiedName> getProperties() {
        return properties;
    }
    
    public DerivePropertyTypeInEntityTypeAclRequest setPrincipal( Principal principal ) {
        this.principal = principal;
        return this;
    }

    public DerivePropertyTypeInEntityTypeAclRequest setType( FullQualifiedName entityTypeFqn ) {
        this.entityTypeFqn = entityTypeFqn;
        return this;
    }

    public DerivePropertyTypeInEntityTypeAclRequest setProperties( Set<FullQualifiedName> properties ) {
        this.properties = properties;
        return this;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        DerivePropertyTypeInEntityTypeAclRequest that = (DerivePropertyTypeInEntityTypeAclRequest) o;

        if ( principal != null ? !principal.equals( that.principal ) : that.principal != null )
            return false;
        if ( entityTypeFqn != null ? !entityTypeFqn.equals( that.entityTypeFqn ) : that.entityTypeFqn != null )
            return false;
        return properties != null ? properties.equals( that.properties ) : that.properties == null;
    }

    @Override
    public int hashCode() {
        int result = principal != null ? principal.hashCode() : 0;
        result = 31 * result + ( entityTypeFqn != null ? entityTypeFqn.hashCode() : 0 );
        result = 31 * result + ( properties != null ? properties.hashCode() : 0 );
        return result;
    }
    
}
