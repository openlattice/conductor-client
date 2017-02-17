package com.dataloom.linking.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.dataloom.linking.LinkingEntityKey;
import com.google.common.base.Preconditions;

public class UnorderedPair<T> {
    private final Set<T> backingCollection = new HashSet<T>( 2 );

    public UnorderedPair( T a, T b ) {
        backingCollection.add( a );
        backingCollection.add( b );
    }
    
    public UnorderedPair( Set<T> pair ){
        Preconditions.checkArgument( pair.size() <= 2, "There are more than two elements in the set." );
        backingCollection.addAll( pair );
    }

    public Set<T> getBackingCollection() {
        return backingCollection;
    }

    public List<T> getAsList() {
        return backingCollection.stream().collect( Collectors.toList() );
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
