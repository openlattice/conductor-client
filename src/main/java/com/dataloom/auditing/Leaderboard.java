package com.dataloom.auditing;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class Leaderboard {
    private final ConcurrentSkipListMap<Integer, Object> leaderboard;
    //Actually cap minus one.
    private final int                                    cap;
    private final AtomicInteger                          size;
    public Leaderboard() {
        this( 99 );
    }

    public Leaderboard( int cap ) {
        this.leaderboard = new ConcurrentSkipListMap<>();
        this.size = new AtomicInteger();
        this.cap = cap;
        ;
    }

    public void tryInsert( int count, List<UUID> value ) {
        leaderboard.put( count, value );
        //If we made the leaderboard too big then we need to shrink it.
        if ( size.incrementAndGet() > cap ) {
            //Only execute deletes for keys that are safely above cap.
            while ( size.decrementAndGet() > cap ) {
                leaderboard.remove( leaderboard.firstKey() );
            }
            //The decrement that would have brought the size of the map back to cap will not execute.
        }
    }

}
