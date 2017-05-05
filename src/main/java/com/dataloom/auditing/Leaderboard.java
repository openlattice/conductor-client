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

package com.dataloom.auditing;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

@Deprecated
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
