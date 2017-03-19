package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.Set;

import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingBiConsumer;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingConsumer;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingFunction;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;

/**
 * Factory method for stream serializing Guava Optionals in hazelcast.
 * 
 * @author Ho Chung Siu
 *
 */
public class OptionalStreamSerializers {
    // Serialize single optional
    public static <T> void serialize( ObjectDataOutput out, Optional<T> element, IoPerformingConsumer<T> c )
            throws IOException {
        final boolean present = element.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            c.accept( element.get() );
        }
    }

    public static <T> void serialize(
            ObjectDataOutput out,
            Optional<T> element,
            IoPerformingBiConsumer<ObjectDataOutput, T> c ) throws IOException {
        final boolean present = element.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            c.accept( out, element.get() );
        }
    }

    public static <T> Optional<T> deserialize( ObjectDataInput in, IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        if ( in.readBoolean() ) {
            T elem = f.apply( in );
            return ( elem == null ) ? Optional.absent() : Optional.of( elem );
        } else {
            return Optional.absent();
        }
    }
    
    // Serialize set of optional
    public static <T> void serializeSet( ObjectDataOutput out, Optional<Set<T>> elements, IoPerformingConsumer<T> c )
            throws IOException {
        final boolean present = elements.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            SetStreamSerializers.serialize( out, elements.get(), c );
        }
    }

    public static <T> void serializeSet(
            ObjectDataOutput out,
            Optional<Set<T>> elements,
            IoPerformingBiConsumer<ObjectDataOutput, T> c ) throws IOException {
        final boolean present = elements.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            SetStreamSerializers.serialize( out, elements.get(), c );
        }
    }
    
    public static <T> Optional<Set<T>> deserializeSet( ObjectDataInput in, IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        if ( in.readBoolean() ) {
            Set<T> elements = SetStreamSerializers.deserialize( in, f );
            return Optional.of( elements );
        } else {
            return Optional.absent();
        }
    }
}
