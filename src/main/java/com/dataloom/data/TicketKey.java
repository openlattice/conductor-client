package com.dataloom.data;

import java.util.UUID;

public class TicketKey {
    private final String principalId;
    private final UUID   ticket;

    public TicketKey( String principalId, UUID ticket ) {
        this.principalId = principalId;
        this.ticket = ticket;
    }

    public TicketKey( String principalId ) {
        this( principalId, UUID.randomUUID() );
    }

    public String getPrincipalId() {
        return principalId;
    }

    public UUID getTicket() {
        return ticket;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( principalId == null ) ? 0 : principalId.hashCode() );
        result = prime * result + ( ( ticket == null ) ? 0 : ticket.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof TicketKey ) ) {
            return false;
        }
        TicketKey other = (TicketKey) obj;
        if ( principalId == null ) {
            if ( other.principalId != null ) {
                return false;
            }
        } else if ( !principalId.equals( other.principalId ) ) {
            return false;
        }
        if ( ticket == null ) {
            if ( other.ticket != null ) {
                return false;
            }
        } else if ( !ticket.equals( other.ticket ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TicketKey [principalId=" + principalId + ", ticket=" + ticket + "]";
    }

}
