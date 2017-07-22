/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.dataloom.data;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class HazelcastStream<T> implements Iterable<T> {
    private static final Logger logger = LoggerFactory.getLogger( HazelcastStream.class );
    private final           UUID      streamId;
    private transient final IQueue<T> stream;
    private final           ILock     streamLock;
    private boolean bufferingIncomplete = true;
    private long    expectedLength      = 0;
    private long    readLength          = 0;

    public HazelcastStream( ListeningExecutorService executor, HazelcastInstance hazelcastInstance ) {
        Pair<UUID, ILock> idAndLock = acquireSafeId( hazelcastInstance );
        this.streamId = idAndLock.getLeft();
        this.streamLock = idAndLock.getRight();
        this.stream = hazelcastInstance.getQueue( streamId.toString() );
        executor.execute( this::startBuffering );
    }

    private Pair<UUID, ILock> acquireSafeId( HazelcastInstance hazelcastInstance ) {
        UUID id;
        ILock maybeStreamLock;
        do {
            id = UUID.randomUUID();
            maybeStreamLock = hazelcastInstance.getLock( id.toString() );
        } while ( !maybeStreamLock.tryLock() );

        return Pair.of( id, maybeStreamLock );
    }

    protected abstract long buffer( UUID streamId );

    private void startBuffering() {
        expectedLength = buffer( streamId );
        bufferingIncomplete = false;
    }

    @Override public Iterator<T> iterator() {
        return new Iterator<T>() {
            private final Iterator<T> i = stream.iterator();

            @Override
            public boolean hasNext() {
                if ( bufferingIncomplete || ( readLength < expectedLength ) ) {
                    return true;
                } else {
                    stream.destroy();
                    streamLock.unlock();
                    streamLock.destroy();
                    return false;
                }
            }

            @Override public T next() {
                try {
                    T next = stream.poll( 10, TimeUnit.MINUTES );
                    return next;
                } catch ( InterruptedException e ) {
                    logger.error( "Unable to retrieve items from hazelcast stream.", e );
                    return null;
                }
            }
        };
    }
}
