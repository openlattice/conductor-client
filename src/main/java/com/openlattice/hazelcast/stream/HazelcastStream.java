package com.openlattice.hazelcast.stream;

import static com.google.common.base.Preconditions.checkState;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class HazelcastStream<T> implements Iterable<T>, HazelcastInstanceAware {
    private static final Map<Class<?>, Logger> subclassLoggers = new HashMap<>();

    private final Logger logger = subclassLoggers.computeIfAbsent( getClass(), LoggerFactory::getLogger );

    private final AtomicBoolean initialized;

    private UUID                     streamId;
    private ILock                    streamLock;
    private IQueue<StreamElement<T>> stream;

    /**
     * This constructor is primarily for serialization and shoul
     */
    protected HazelcastStream() {
        initialized = new AtomicBoolean( false );
    }

    protected HazelcastStream( HazelcastInstance hazelcastInstance ) {
        this();
        init( hazelcastInstance );
    }

    @Override public Iterator<T> iterator() {
        checkState( initialized.get(), "Cannot acquire iterator for uninitialized stream." );
        HazelcastIterator hzIterator = new HazelcastIterator<T>( streamLock, streamId, stream );
        return hzIterator;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        init( hazelcastInstance );
    }

    public UUID getStreamId() {
        return streamId;
    }

    public ILock getStreamLock() {
        return streamLock;
    }

    public IQueue<StreamElement<T>> getStream() {
        return stream;
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

    private void init( HazelcastInstance hazelcastInstance ) {
        Pair<UUID, ILock> idAndLock = acquireSafeId( hazelcastInstance );
        this.streamId = idAndLock.getLeft();
        this.streamLock = idAndLock.getRight();
        this.stream = hazelcastInstance.getQueue( streamId.toString() );
        initialized.set( true );
    }

    public static class HazelcastIterator<T> implements Iterator<T> {
        private static final Logger logger = LoggerFactory.getLogger( HazelcastIterator.class );
        private final ILock                    streamLock;
        private final UUID                     streamId;
        private final IQueue<StreamElement<T>> stream;
        private final Lock lock = new ReentrantLock();
        private StreamElement<T> next;

        public HazelcastIterator(
                ILock streamLock,
                UUID streamId,
                IQueue<StreamElement<T>> stream ) {
            this.streamLock = streamLock;
            this.streamId = streamId;
            this.stream = stream;
        }

        public UUID getStreamId() {
            return streamId;
        }

        @Override
        public boolean hasNext() {
            final boolean eof;
            //Try for safety, but unnecessary as no exceptions would be thrown.
            try {
                lock.lock();
                eof = next.isEof();
            } finally {
                lock.unlock();
            }
            //If we have found that end of stream rel
            if ( eof ) {
                releaseId();
                return false;
            }
            return true;
        }

        private void releaseId() {
            stream.destroy();
            streamLock.unlock();
            streamLock.destroy();
        }

        @Override
        public T next() {
            T nextElem = next.get();

            try {
                lock.lock();
                next = stream.poll( 60, TimeUnit.SECONDS );
                if ( next == null ) {
                    next = StreamElement.eof();
                }
            } catch ( InterruptedException e ) {
                logger.error( "Unable to retrieve next element from stream queue {}", streamId, e );
                next = StreamElement.eof();
            } finally {
                lock.unlock();
                ;
            }

            if ( next.isEof() ) {
                releaseId();
            }

            return nextElem;
        }
    }
}
