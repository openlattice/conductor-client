package com.dataloom.graph.core.impl;

import java.util.UUID;

import com.dataloom.data.EntityKey;

/**
 * Contains information about vertex/edge in a graph. This includes identifying information (Id and type), and any extra
 * information. 
 * 
 * In the current setup, the extra info is the EntityKey where this label comes from.
 * <ul>
 * <li>For a vertex, this entity key comes from the relevant entity table (e.g. Person).</li>
 * <li>For an edge, this entity key comes from the relevant event table (e.g. Call For Service).</li>
 * </ul>
 * 
 * @author Ho Chung Siu
 *
 */
public class Label {
    private UUID      id;
    private Type      type;

    private EntityKey ref;

    public Label( UUID id, Type type, EntityKey ref ) {
        this.id = id;
        this.type = type;
        this.ref = ref;
    }

    public UUID getId() {
        return id;
    }

    public Type getType() {
        return type;
    }

    public EntityKey getRef() {
        return ref;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( id == null ) ? 0 : id.hashCode() );
        result = prime * result + ( ( ref == null ) ? 0 : ref.hashCode() );
        result = prime * result + ( ( type == null ) ? 0 : type.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Label other = (Label) obj;
        if ( id == null ) {
            if ( other.id != null ) return false;
        } else if ( !id.equals( other.id ) ) return false;
        if ( ref == null ) {
            if ( other.ref != null ) return false;
        } else if ( !ref.equals( other.ref ) ) return false;
        if ( type != other.type ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "Label [id=" + id + ", type=" + type + ", ref=" + ref + "]";
    }

    public enum Type {
        VERTEX,
        EDGE
    }

}
