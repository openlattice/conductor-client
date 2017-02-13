package com.dataloom.linking.util;

import java.util.HashSet;
import java.util.Set;

public class UnorderedPair<T> {
    private final Set<T> backingCollection = new HashSet<T>( 2 );

    UnorderedPair( T a, T b ) {
        backingCollection.add( a );
        backingCollection.add( b );
    }

    public Set<T> getElements() {
        return backingCollection;
    }

    @Override
    public int hashCode() {
        return backingCollection.hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( !( obj instanceof UnorderedPair<?> ) ) return false;
        return backingCollection.equals( ( (UnorderedPair<?>) obj ).backingCollection );
    }

    @Override
    public String toString() {
        return "UnorderedPair backed by " + backingCollection;
    }

}
