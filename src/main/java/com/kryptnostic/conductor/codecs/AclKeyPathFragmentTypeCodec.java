package com.kryptnostic.conductor.codecs;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class AclKeyPathFragmentTypeCodec extends TypeCodec<UUID> {

    public AclKeyPathFragmentTypeCodec() {
        super( DataType.blob(), UUID.class );
    }

    @Override
    public UUID deserialize( ByteBuffer bytes, ProtocolVersion ver ) throws InvalidTypeException {
        if ( bytes == null ) {
            return null;
        }
        ByteBuffer dup = bytes.duplicate();
        long lsb = dup.getLong();
        long msb = dup.getLong();
        return new UUID( msb, lsb );
    }

    @Override
    public String format( UUID value ) throws InvalidTypeException {
        return value.toString();

    }

    @Override
    public UUID parse( String value ) throws InvalidTypeException {
        return UUID.fromString( value );
    }

    @Override
    public ByteBuffer serialize( UUID value, ProtocolVersion ver ) throws InvalidTypeException {
        if ( value == null ) {
            return null;
        }
        final ByteBuffer buf = ByteBuffer.allocate( Long.BYTES << 1 );
        buf.putLong( value.getLeastSignificantBits() );
        buf.putLong( value.getMostSignificantBits() );
        buf.rewind();
        buf.clear();
        return buf;
    }

}
