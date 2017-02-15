package com.kryptnostic.conductor.codecs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EntityKeyTypeCodec extends TypeCodec<EntityKey> {
    
    private static final Logger     logger = LoggerFactory
            .getLogger( EntityKeyTypeCodec.class );

    private final ObjectMapper mapper;
    
    public EntityKeyTypeCodec( ObjectMapper mapper ) {
        super( DataType.blob(), EntityKey.class );
        this.mapper = mapper;
    }

    @Override
    public ByteBuffer serialize( EntityKey value, ProtocolVersion protocolVersion ) throws InvalidTypeException {
        return serialize( value );
    }

    @Override
    public EntityKey deserialize( ByteBuffer bytes, ProtocolVersion protocolVersion ) throws InvalidTypeException {
        return deserialize( bytes );
    }

    @Override
    public EntityKey parse( String value ) throws InvalidTypeException {
        return deserialize( TypeCodec.blob().parse( value ) );
    }

    @Override
    public String format( EntityKey value ) throws InvalidTypeException {
        return TypeCodec.blob().format( serialize( value ) );
    }

    private ByteBuffer serialize( EntityKey value ) throws InvalidTypeException { 
        if( value == null ) return null;
        try {
            return ByteBuffer.wrap( mapper.writeValueAsBytes( value ) );
        } catch ( JsonProcessingException e ) {
            logger.error( "Failed to serialize entity key: " + value );
            return null;
        }
    }
    
    private EntityKey deserialize( ByteBuffer bytes ) throws InvalidTypeException {
        if( bytes == null ) return null;
        try {
            return mapper.readValue( Bytes.getArray( bytes ), EntityKey.class );
        } catch ( IOException e ) {
            logger.error( "Failed to deserialize entity key." );
            return null;
        }
    }

}
