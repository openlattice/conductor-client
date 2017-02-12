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

package com.dataloom.data;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DelegatedEntityKeySet implements Set<EntityKey> {
    private final Set<EntityKey> entityKeys;

    public DelegatedEntityKeySet( Set<EntityKey> entityKeys ) {
        this.entityKeys = entityKeys;
    }

    @Override public int size() {
        return entityKeys.size();
    }

    @Override public boolean isEmpty() {
        return entityKeys.isEmpty();
    }

    @Override public boolean contains( Object o ) {
        return entityKeys.contains( o );
    }

    @Override public Iterator<EntityKey> iterator() {
        return entityKeys.iterator();
    }

    @Override public Object[] toArray() {
        return entityKeys.toArray();
    }

    @Override public <T> T[] toArray( T[] a ) {
        return entityKeys.toArray( a );
    }

    @Override public boolean add( EntityKey entityKey ) {
        return entityKeys.add( entityKey );
    }

    @Override public boolean remove( Object o ) {
        return entityKeys.remove( o );
    }

    @Override public boolean containsAll( Collection<?> c ) {
        return entityKeys.containsAll( c );
    }

    @Override public boolean addAll( Collection<? extends EntityKey> c ) {
        return entityKeys.addAll( c );
    }

    @Override public boolean retainAll( Collection<?> c ) {
        return entityKeys.retainAll( c );
    }

    @Override public boolean removeAll( Collection<?> c ) {
        return entityKeys.removeAll( c );
    }

    @Override public void clear() {
        entityKeys.clear();
    }

    @Override public boolean equals( Object o ) {
        return entityKeys.equals( o );
    }

    @Override public int hashCode() {
        return entityKeys.hashCode();
    }

    @Override public Spliterator<EntityKey> spliterator() {
        return entityKeys.spliterator();
    }

    @Override public boolean removeIf( Predicate<? super EntityKey> filter ) {
        return entityKeys.removeIf( filter );
    }

    @Override public Stream<EntityKey> stream() {
        return entityKeys.stream();
    }

    @Override public Stream<EntityKey> parallelStream() {
        return entityKeys.parallelStream();
    }

    @Override public void forEach( Consumer<? super EntityKey> action ) {
        entityKeys.forEach( action );
    }

}
