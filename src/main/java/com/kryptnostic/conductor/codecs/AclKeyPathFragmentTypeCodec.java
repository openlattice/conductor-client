package com.kryptnostic.conductor.codecs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.SecurableObjectType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class AclKeyPathFragmentTypeCodec extends TypeCodec<AclKeyPathFragment> {

    public AclKeyPathFragmentTypeCodec() {
        super( DataType.blob(), AclKeyPathFragment.class );
    }

    @Override
    public AclKeyPathFragment deserialize( ByteBuffer bytes, ProtocolVersion ver ) throws InvalidTypeException {
        if ( bytes == null ) {
            return null;
        }
        byte[] b = new byte[ bytes.remaining() - ( Long.BYTES << 1 ) ];
        ByteBuffer dup = bytes.duplicate();
        dup.get( b );
        long lsb = dup.getLong();
        long msb = dup.getLong();
        return new AclKeyPathFragment(
                SecurableObjectType.valueOf( new String( b, StandardCharsets.UTF_8 ) ),
                new UUID( msb, lsb ) );
    }

    @Override
    public String format( AclKeyPathFragment value ) throws InvalidTypeException {
        return value.getType() + "," + value.getId().toString();

    }

    @Override
    public AclKeyPathFragment parse( String value ) throws InvalidTypeException {
        String[] parts = value.split( "," );
        return new AclKeyPathFragment( SecurableObjectType.valueOf( parts[ 0 ] ), UUID.fromString( parts[ 1 ] ) );
    }

    @Override
    public ByteBuffer serialize( AclKeyPathFragment value, ProtocolVersion ver ) throws InvalidTypeException {
        if ( value == null ) {
            return null;
        }
        final byte[] fqnBytes = value.getType().name().getBytes( StandardCharsets.UTF_8 );
        final ByteBuffer buf = ByteBuffer.allocate( fqnBytes.length + ( Long.BYTES << 1 ) );
        final UUID id = value.getId();
        buf.put( fqnBytes );
        buf.putLong( id.getLeastSignificantBits() );
        buf.putLong( id.getMostSignificantBits() );
        buf.rewind();
        buf.clear();
        return buf;
    }

}
