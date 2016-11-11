package com.kryptnostic.datastore.services.requests;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.Principal;

public class DeriveEntitySetAclRequest {
    protected Principal principal;
    protected FullQualifiedName entityTypeFqn;
    protected String entitySetName;
    
    @JsonCreator
    public DeriveEntitySetAclRequest createAclRequest(
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal Principal,
            @JsonProperty( SerializationConstants.TYPE_FIELD) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.NAME_FIELD) String entitySetName ){
        return new DeriveEntitySetAclRequest().setPrincipal( principal ).setType( entityTypeFqn ).setName( entitySetName );
    }
    
    public Principal getPrincipal() {
        return principal;
    }

    public FullQualifiedName getType() {
        return entityTypeFqn;
    }
    
    public String getName() {
        return entitySetName;
    }
    
    public DeriveEntitySetAclRequest setPrincipal( Principal principal ) {
        this.principal = principal;
        return this;
    }

    public DeriveEntitySetAclRequest setType( FullQualifiedName entityTypeFqn ) {
        this.entityTypeFqn = entityTypeFqn;
        return this;
    }

    public DeriveEntitySetAclRequest setName( String entitySetName ) {
        this.entitySetName = entitySetName;
        return this;
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        DeriveEntitySetAclRequest that = (DeriveEntitySetAclRequest) o;

        if ( principal != null ? !principal.equals( that.principal ) : that.principal != null )
            return false;
        if ( entityTypeFqn != null ? !entityTypeFqn.equals( that.entityTypeFqn ) : that.entityTypeFqn != null )
            return false;
        return entitySetName != null ? entitySetName.equals( that.entitySetName ) : that.entitySetName == null;
    }

    @Override
    public int hashCode() {
        int result = principal != null ? principal.hashCode() : 0;
        result = 31 * result + ( entityTypeFqn != null ? entityTypeFqn.hashCode() : 0 );
        result = 31 * result + ( entitySetName != null ? entitySetName.hashCode() : 0 );
        return result;
    }

}
