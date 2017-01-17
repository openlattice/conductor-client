package com.dataloom.organizations;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.dataloom.authorization.Principal;

public class PrincipalSet implements Set<Principal>, Serializable {
    private static final long serialVersionUID = 1851088039565151792L;
    private Set<Principal>    principals;

    public PrincipalSet( Set<Principal> principals ) {
        this.principals = principals;
    }

    public static PrincipalSet wrap( Set<Principal> principals ) {
        return new PrincipalSet( principals );
    }

    public Set<Principal> unwrap() {
        return principals;
    }

    public void forEach( Consumer<? super Principal> action ) {
        principals.forEach( action );
    }

    public int size() {
        return principals.size();
    }

    public boolean isEmpty() {
        return principals.isEmpty();
    }

    public boolean contains( Object o ) {
        return principals.contains( o );
    }

    public Iterator<Principal> iterator() {
        return principals.iterator();
    }

    public Object[] toArray() {
        return principals.toArray();
    }

    public <T> T[] toArray( T[] a ) {
        return principals.toArray( a );
    }

    public boolean add( Principal e ) {
        return principals.add( e );
    }

    public boolean remove( Object o ) {
        return principals.remove( o );
    }

    public boolean containsAll( Collection<?> c ) {
        return principals.containsAll( c );
    }

    public boolean addAll( Collection<? extends Principal> c ) {
        return principals.addAll( c );
    }

    public boolean retainAll( Collection<?> c ) {
        return principals.retainAll( c );
    }

    public boolean removeAll( Collection<?> c ) {
        return principals.removeAll( c );
    }

    public void clear() {
        principals.clear();
    }

    public boolean equals( Object o ) {
        return principals.equals( o );
    }

    public int hashCode() {
        return principals.hashCode();
    }

    public Spliterator<Principal> spliterator() {
        return principals.spliterator();
    }

    public boolean removeIf( Predicate<? super Principal> filter ) {
        return principals.removeIf( filter );
    }

    public Stream<Principal> stream() {
        return principals.stream();
    }

    public Stream<Principal> parallelStream() {
        return principals.parallelStream();
    }

}
