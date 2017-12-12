package com.dataloom.graph.core.objects;

import com.google.common.base.Preconditions;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class NeighborTripletSet implements Set<DelegatedUUIDList> {

    private final Set<DelegatedUUIDList> triplets;

    public NeighborTripletSet( Set<DelegatedUUIDList> triplets ) {
        this.triplets = triplets;
    }

    @Override public int size() {
        return triplets.size();
    }

    @Override public boolean isEmpty() {
        return triplets.isEmpty();
    }

    @Override public boolean contains( Object o ) {
        return triplets.contains( o );
    }

    @Override public Iterator<DelegatedUUIDList> iterator() {
        return triplets.iterator();
    }

    @Override public Object[] toArray() {
        return triplets.toArray();
    }

    @Override public <T> T[] toArray( T[] a ) {
        return triplets.toArray( a );
    }

    @Override public boolean add( DelegatedUUIDList uuids ) {
        Preconditions.checkArgument( uuids.size() == 3, "A triplet must contain exactly three elements." );
        return triplets.add( uuids );
    }

    @Override public boolean remove( Object o ) {
        return triplets.remove( o );
    }

    @Override public boolean containsAll( Collection<?> c ) {
        return triplets.removeAll( c );
    }

    @Override public boolean addAll( Collection<? extends DelegatedUUIDList> c ) {
        return triplets.addAll( c );
    }

    @Override public boolean retainAll( Collection<?> c ) {
        return triplets.retainAll( c );
    }

    @Override public boolean removeAll( Collection<?> c ) {
        return triplets.removeAll( c );
    }

    @Override public void clear() {
        triplets.clear();
    }

    @Override public boolean equals( Object o ) {
        return triplets.equals( o );
    }

    @Override public int hashCode() {
        return triplets.hashCode();
    }
}
