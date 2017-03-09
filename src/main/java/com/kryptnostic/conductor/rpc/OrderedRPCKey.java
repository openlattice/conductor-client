package com.kryptnostic.conductor.rpc;

import java.util.UUID;

public class OrderedRPCKey {
    private final UUID requestId;
    private final double weight;
    
    public OrderedRPCKey( UUID requestId, double weight ) {
        this.requestId = requestId;
        this.weight = weight;
    }
    
    public UUID getRequestId() {
        return requestId;
    }
    
    public double getWeight() {
        return weight;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( requestId == null ) ? 0 : requestId.hashCode() );
        long temp;
        temp = Double.doubleToLongBits( weight );
        result = prime * result + (int) ( temp ^ ( temp >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        OrderedRPCKey other = (OrderedRPCKey) obj;
        if ( requestId == null ) {
            if ( other.requestId != null ) return false;
        } else if ( !requestId.equals( other.requestId ) ) return false;
        if ( Double.doubleToLongBits( weight ) != Double.doubleToLongBits( other.weight ) ) return false;
        return true;
    }

}
