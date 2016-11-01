package com.kryptnostic.conductor.rpc.odata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;

public class DetailedEntityType {

    private final String                 namespace;
    private final String                 name;
    private final Set<FullQualifiedName> key;
    private final Set<PropertyType>      properties;
    private final Set<FullQualifiedName> schemas;

    @JsonCreator
    public DetailedEntityType(
            @JsonProperty( SerializationConstants.NAMESPACE_FIELD ) String namespace,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.KEY_FIELD ) Set<FullQualifiedName> key,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) Set<PropertyType> properties,
            @JsonProperty( SerializationConstants.SCHEMAS ) Optional<Set<FullQualifiedName>> schemas ) {
        this.namespace = namespace;
        this.name = name;
        this.key = key;
        this.properties = properties;
        this.schemas = schemas.or( ImmutableSet.of() );
    }

    @JsonProperty( SerializationConstants.NAMESPACE_FIELD )
    public String getNamespace() {
        return namespace;
    }

    @JsonProperty( SerializationConstants.NAME_FIELD )
    public String getName() {
        return name;
    }

    @JsonProperty( SerializationConstants.KEY_FIELD )
    public Set<FullQualifiedName> getKey() {
        return key;
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public Set<PropertyType> getProperties() {
        return properties;
    }

    @JsonProperty( SerializationConstants.SCHEMAS )
    public Set<FullQualifiedName> getSchemas() {
        return schemas;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        DetailedEntityType that = (DetailedEntityType) o;

        if ( !namespace.equals( that.namespace ) )
            return false;
        if ( !name.equals( that.name ) )
            return false;
        if ( !key.equals( that.key ) )
            return false;
        if ( properties != null ? !properties.equals( that.properties ) : that.properties != null )
            return false;
        return schemas != null ? schemas.equals( that.schemas ) : that.schemas == null;

    }

    @Override public int hashCode() {
        int result = namespace.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + ( properties != null ? properties.hashCode() : 0 );
        result = 31 * result + ( schemas != null ? schemas.hashCode() : 0 );
        return result;
    }

    @Override public String toString() {
        return "DetailedEntityType{" +
                "namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", key=" + key +
                ", properties=" + properties +
                ", schemas=" + schemas +
                '}';
    }
}
