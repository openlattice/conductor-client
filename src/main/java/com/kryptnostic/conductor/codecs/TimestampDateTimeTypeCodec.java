package com.kryptnostic.conductor.codecs;

import java.nio.ByteBuffer;

import org.joda.time.DateTime;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * TypeCodec between Cql Timestamp and Joda DateTime, modified from default TimestampCodec
 * @author Ho Chung Siu
 *
 */
public class TimestampDateTimeTypeCodec extends TypeCodec<DateTime> {
    private static final TimestampDateTimeTypeCodec instance = new TimestampDateTimeTypeCodec();
    
    private TimestampDateTimeTypeCodec() {
        super(DataType.timestamp(), DateTime.class);
    }
    
    public static TimestampDateTimeTypeCodec getInstance(){
        return instance;
    }
    
    @Override
    public DateTime parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        // strip enclosing single quotes, if any
        if (ParseUtils.isQuoted(value))
            value = ParseUtils.unquote(value);

        if (ParseUtils.isLongLiteral(value)) {
            try {
                return new DateTime(Long.parseLong(value));
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }

        try {
            return DateTime.parse( value );
        } catch (IllegalArgumentException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }

    @Override
    public String format(DateTime value) {
        if (value == null)
            return "NULL";
        return Long.toString(value.getMillis());
    }
    
    @Override
    public ByteBuffer serialize(DateTime value, ProtocolVersion protocolVersion) {
        return value == null ? null : TypeCodec.bigint().serializeNoBoxing(value.getMillis(), protocolVersion);
    }
    
    @Override
    public DateTime deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null || bytes.remaining() == 0 ? null : new DateTime(TypeCodec.bigint().deserializeNoBoxing(bytes, protocolVersion));
    }
}
