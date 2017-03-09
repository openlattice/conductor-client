package com.kryptnostic.conductor.rpc;

import java.util.Iterator;
import java.util.UUID;

import com.hazelcast.core.IMap;

public class RPCIteratorOrdered<V> implements Iterator<V> {
    private UUID requestId;
    private double weight;
    private V value;
    private IMap<OrderedRPCKey, V> map;
    
    public RPCIteratorOrdered( UUID requestId, IMap<OrderedRPCKey, V> map ) {
        this.requestId = requestId;
        this.map = map;
    }

    @Override
    public boolean hasNext() {
        OrderedRPCKey nextKey = new OrderedRPCKey( requestId, weight );
        return map.containsKey( nextKey ) && map.get( nextKey ) != null;
    }
    @Override
    public V next() {
        V result = map.get( new OrderedRPCKey( requestId, weight ) );
        //index++;
        return result;
    }
}
