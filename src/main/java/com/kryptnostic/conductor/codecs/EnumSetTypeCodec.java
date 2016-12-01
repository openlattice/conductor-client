package com.kryptnostic.conductor.codecs;

import java.util.EnumSet;

import com.dataloom.authorization.requests.Permission;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractCollectionCodec;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

public class EnumSetTypeCodec<T extends Enum<T>> extends AbstractCollectionCodec<T, EnumSet<T>> {

    //Specify cqlType and codec for Permission
    private Class<T> javaType;
    
    public EnumSetTypeCodec( TypeCodec<T> eltCodec ){
        super(DataType.set(eltCodec.getCqlType()), getTypeToken( eltCodec.getJavaType() ) , eltCodec);
        javaType = (Class<T>) eltCodec.getJavaType().getRawType();
    }
    
    private static <T extends Enum<T>> TypeToken<EnumSet<T>> getTypeToken( TypeToken<T> eltType ) {
        return new TypeToken<EnumSet<T>>(){
            private static final long serialVersionUID = -6972369825357521908L;
        }.where( new TypeParameter<T>(){}, eltType);
    }

    @Override
    protected EnumSet<T> newInstance( int size ) {
        return EnumSet.noneOf( javaType );
    }

    //Type Token for Permission, the main use case
    private static final TypeToken< EnumSet<Permission> > enumSetPermission = new TypeToken<EnumSet<Permission>>(){};
    
    public static TypeToken< EnumSet<Permission> > getTypeTokenForEnumSetPermission () {
        return enumSetPermission;
    }
    
}
