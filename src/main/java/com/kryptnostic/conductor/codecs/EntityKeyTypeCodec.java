package com.kryptnostic.conductor.codecs;

import com.dataloom.data.EntityKey;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class EntityKeyTypeCodec extends TypeCodec<EntityKey> {

    private static final Logger logger = LoggerFactory
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
        if ( value == null ) {
            return null;
        }
        final byte[] idBytes = value.getEntityId().getBytes();
        final int len = 2 * Long.BYTES + idBytes.length;
        final byte[] bytes = new byte[ len ];
        final ByteBuffer buf = ByteBuffer.wrap( bytes );
        buf.putLong( value.getEntitySetId().getLeastSignificantBits() );
        buf.putLong( value.getEntitySetId().getMostSignificantBits() );
        buf.put( idBytes );
        return buf.duplicate();
    }

    private EntityKey deserialize( ByteBuffer bytes ) throws InvalidTypeException {
        if ( bytes == null ) {
            return null;
        }
        ByteBuffer buf = bytes.duplicate();
        long lsb = bytes.getLong();
        long msb = bytes.getLong();
        UUID entitySetId = new UUID( msb, lsb );
        byte[] idBytes = new byte[ buf.remaining() ];
        String entityId = new String( idBytes, StandardCharsets.UTF_8 );
        return new EntityKey( entitySetId, entityId );
    }

}
