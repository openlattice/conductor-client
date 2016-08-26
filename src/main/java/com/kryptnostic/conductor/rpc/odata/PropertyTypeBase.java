package com.kryptnostic.conductor.rpc.odata;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.datastax.driver.mapping.annotations.Column;

public class PropertyTypeBase extends TypePK {

    @Column(
        name = "datatype" )
    protected EdmPrimitiveTypeKind datatype;

    public PropertyTypeBase setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    public PropertyTypeBase setName( String name ) {
        this.name = name;
        return this;
    }

    public EdmPrimitiveTypeKind getDatatype() {
        return datatype;
    }

    public PropertyTypeBase setDatatype( EdmPrimitiveTypeKind datatype ) {
        this.datatype = datatype;
        return this;
    }

    @Override
    public String toString() {
        return "PropertyTypeBase [datatype=" + datatype + ", namespace=" + namespace + ", name=" + name + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ( ( datatype == null ) ? 0 : datatype.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !super.equals( obj ) ) {
            return false;
        }
        if ( !( obj instanceof PropertyTypeBase ) ) {
            return false;
        }
        PropertyTypeBase other = (PropertyTypeBase) obj;
        if ( datatype != other.datatype ) {
            return false;
        }
        return true;
    }

}
