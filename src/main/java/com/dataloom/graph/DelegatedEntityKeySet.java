/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.graph;

import com.dataloom.data.EntityKey;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DelegatedEntityKeySet {
    private final Set<EntityKey> entityKeys;

    public DelegatedEntityKeySet( Set<EntityKey> entityKeys ) {
        this.entityKeys = entityKeys;
    }

    public int size() {
        return entityKeys.size();
    }

    public boolean isEmpty() {
        return entityKeys.isEmpty();
    }

    public boolean contains( Object o ) {
        return entityKeys.contains( o );
    }

    public Iterator<EntityKey> iterator() {
        return entityKeys.iterator();
    }

    public Object[] toArray() {
        return entityKeys.toArray();
    }

    public <T> T[] toArray( T[] a ) {
        return entityKeys.toArray( a );
    }

    public boolean add( EntityKey entityKey ) {
        return entityKeys.add( entityKey );
    }

    public boolean remove( Object o ) {
        return entityKeys.remove( o );
    }

    public boolean containsAll( Collection<?> c ) {
        return entityKeys.containsAll( c );
    }

    public boolean addAll( Collection<? extends EntityKey> c ) {
        return entityKeys.addAll( c );
    }

    public boolean retainAll( Collection<?> c ) {
        return entityKeys.retainAll( c );
    }

    public boolean removeAll( Collection<?> c ) {
        return entityKeys.removeAll( c );
    }

    public void clear() {
        entityKeys.clear();
    }

    @Override public boolean equals( Object o ) {
        return entityKeys.equals( o );
    }

    @Override public int hashCode() {
        return entityKeys.hashCode();
    }

    public Spliterator<EntityKey> spliterator() {
        return entityKeys.spliterator();
    }

    public boolean removeIf( Predicate<? super EntityKey> filter ) {
        return entityKeys.removeIf( filter );
    }

    public Stream<EntityKey> stream() {
        return entityKeys.stream();
    }

    public Stream<EntityKey> parallelStream() {
        return entityKeys.parallelStream();
    }

    public void forEach( Consumer<? super EntityKey> action ) {
        entityKeys.forEach( action );
    }

    public static DelegatedEntityKeySet wrap( Set<EntityKey> entityKeys ) {
        return new DelegatedEntityKeySet( entityKeys );
    }
}