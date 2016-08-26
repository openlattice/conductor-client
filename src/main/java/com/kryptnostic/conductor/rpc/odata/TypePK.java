package com.kryptnostic.conductor.rpc.odata;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Transient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.kryptnostic.rhizome.mapstores.cassandra.CassandraKey;

public class TypePK implements CassandraKey {
    @PartitionKey(
        value = 0 )
    protected String namespace;
    @ClusteringColumn(
        value = 0 )
    protected String name;

    public String getNamespace() {
        return namespace;
    }

    public TypePK setNamespace( String namespace ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( namespace ), "Namespace cannot be null" );
        this.namespace = namespace;
        return this;
    }

    public String getName() {
        return name;
    }

    public TypePK setName( String name ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( name ), "Name cannot be null" );
        this.name = name;
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( name == null ) ? 0 : name.hashCode() );
        result = prime * result + ( ( namespace == null ) ? 0 : namespace.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof TypePK ) ) {
            return false;
        }
        TypePK other = (TypePK) obj;
        if ( name == null ) {
            if ( other.name != null ) {
                return false;
            }
        } else if ( !name.equals( other.name ) ) {
            return false;
        }
        if ( namespace == null ) {
            if ( other.namespace != null ) {
                return false;
            }
        } else if ( !namespace.equals( other.namespace ) ) {
            return false;
        }
        return true;
    }

    @JsonIgnore
    @Transient
    public FullQualifiedName getFullQualifiedName() {
        return new FullQualifiedName( namespace, name );
    }

    @Override
    public Object[] asPrimaryKey() {
        return new Object[] { namespace, name };
    }
}
