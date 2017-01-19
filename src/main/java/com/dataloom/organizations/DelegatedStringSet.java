package com.dataloom.organizations;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class DelegatedStringSet implements Set<String>, Serializable {
    private static final long serialVersionUID = 1595791228332993996L;
    private final Set<String> emailDomains;

    public DelegatedStringSet( Set<String> emailDomains ) {
        this.emailDomains = emailDomains;
    }

    public static DelegatedStringSet wrap( Set<String> emailDomains ) {
        return new DelegatedStringSet( emailDomains );
    }

    public Set<String> unwrap() {
        return emailDomains;
    }

    public void forEach( Consumer<? super String> action ) {
        emailDomains.forEach( action );
    }

    public int size() {
        return emailDomains.size();
    }

    public boolean isEmpty() {
        return emailDomains.isEmpty();
    }

    public boolean contains( Object o ) {
        return emailDomains.contains( o );
    }

    public Iterator<String> iterator() {
        return emailDomains.iterator();
    }

    public Object[] toArray() {
        return emailDomains.toArray();
    }

    public <T> T[] toArray( T[] a ) {
        return emailDomains.toArray( a );
    }

    public boolean add( String e ) {
        return emailDomains.add( e );
    }

    public boolean remove( Object o ) {
        return emailDomains.remove( o );
    }

    public boolean containsAll( Collection<?> c ) {
        return emailDomains.containsAll( c );
    }

    public boolean addAll( Collection<? extends String> c ) {
        return emailDomains.addAll( c );
    }

    public boolean retainAll( Collection<?> c ) {
        return emailDomains.retainAll( c );
    }

    public boolean removeAll( Collection<?> c ) {
        return emailDomains.removeAll( c );
    }

    public void clear() {
        emailDomains.clear();
    }

    public boolean equals( Object o ) {
        return emailDomains.equals( o );
    }

    public int hashCode() {
        return emailDomains.hashCode();
    }

    public Spliterator<String> spliterator() {
        return emailDomains.spliterator();
    }

    public boolean removeIf( Predicate<? super String> filter ) {
        return emailDomains.removeIf( filter );
    }

    public Stream<String> stream() {
        return emailDomains.stream();
    }

    public Stream<String> parallelStream() {
        return emailDomains.parallelStream();
    }
}
