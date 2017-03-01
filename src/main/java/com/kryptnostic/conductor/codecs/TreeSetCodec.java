package com.kryptnostic.conductor.codecs;

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractCollectionCodec;
import com.datastax.driver.core.TypeTokens;

public class TreeSetCodec<T> extends AbstractCollectionCodec<T, Set<T>> {

    private static final TreeSetCodec<UUID> uuidInstance = new TreeSetCodec<UUID>( TypeCodec.uuid() );
    
    public TreeSetCodec( TypeCodec<T> eltCodec ) {
        super( DataType.set( eltCodec.getCqlType() ), TypeTokens.setOf( eltCodec.getJavaType() ), eltCodec );
    }

    @Override
    protected Set<T> newInstance( int size ) {
        return new TreeSet<T>();
    }

    public static TreeSetCodec<UUID> getUUIDInstance(){
        return uuidInstance;
    }
}
