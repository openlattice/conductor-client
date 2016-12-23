package com.dataloom.hazelcast;

import com.hazelcast.core.IMap;

public class HazelcastUtils {
    public static <T, V> V typedGet( IMap<T, V> m, T key ) {
        return m.get( key );
    }
}
