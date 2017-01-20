package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.dataloom.authorization.Permission;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingConsumer;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingFunction;

public class StreamSerializerUtils {
    private StreamSerializerUtils() {};

    private static final Permission[] permissions = Permission.values();

    public static <T> void serializeFromList( ObjectDataOutput out, List<T> elements, IoPerformingConsumer<T> c )
            throws IOException {
        out.writeInt( elements.size() );
        for ( T elem : elements ) {
            c.accept( elem );
        }
    }

    public static <T> List<T> deserializeToList(
            ObjectDataInput in,
            List<T> list,
            int size,
            IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        for ( int i = 0; i < size; ++i ) {
            T elem = f.apply( in );
            if ( elem != null ) {
                list.add( elem );
            }
        }
        return list;
    }

    public static <T> List<T> deserializeToList( ObjectDataInput in, IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        int size = in.readInt();
        return deserializeToList( in, Lists.newArrayListWithExpectedSize( size ), size, f );
    }

    public static <K, V> void serializeFromMap(
            ObjectDataOutput out,
            Map<K, V> elements,
            IoPerformingConsumer<K> cK,
            IoPerformingConsumer<V> cV )
            throws IOException {
        out.writeInt( elements.size() );
        for ( Map.Entry<K, V> elem : elements.entrySet() ) {
            cK.accept( elem.getKey() );
            cV.accept( elem.getValue() );
        }
    }

    public static <K, V> Map<K, V> deserializeToMap(
            ObjectDataInput in,
            Map<K, V> map,
            int size,
            IoPerformingFunction<ObjectDataInput, K> fK,
            IoPerformingFunction<ObjectDataInput, V> fV )
            throws IOException {
        for ( int i = 0; i < size; ++i ) {
            K key = fK.apply( in );
            V value = fV.apply( in );
            if ( key != null ) {
                map.put( key, value );
            }
        }
        return map;
    }

    public static <K, V> Map<K, V> deserializeToMap(
            ObjectDataInput in,
            IoPerformingFunction<ObjectDataInput, K> fK,
            IoPerformingFunction<ObjectDataInput, V> fV )
            throws IOException {
        int size = in.readInt();
        return deserializeToMap( in, Maps.newHashMapWithExpectedSize( size ), size, fK, fV );
    }

    public static void serializeFromPermissionEnumSet( ObjectDataOutput out, EnumSet<Permission> object ) throws IOException {
        out.writeInt( object.size() );
        for ( Permission permission : object ) {
            out.writeInt( permission.ordinal() );
        }
    }

    public static EnumSet<Permission> deserializeToPermissionEnumSet( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        EnumSet<Permission> set = EnumSet.noneOf( Permission.class );
        
        for ( int i = 0; i < size; ++i ) {
            set.add( permissions[ in.readInt() ] );
        }
        return set;
    }

}
