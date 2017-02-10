package com.dataloom.linking;

import java.util.Map;

import com.dataloom.data.EntityKey;

public class Entity {
    private EntityKey           key;
    private Map<String, Object> properties;

    public Entity( EntityKey key, Map<String, Object> properties ) {
        this.key = key;
        this.properties = properties;
    }

    public EntityKey getKey() {
        return key;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public int hashCode() {
        // Entity Key should decide uniqueness of entity, so it suffices to check equality/write hashcode for entity
        // key.
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        // Entity Key should decide uniqueness of entity, so it suffices to check equality/write hashcode for entity
        // key.
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Entity other = (Entity) obj;
        if ( key == null ) {
            if ( other.key != null ) return false;
        } else if ( !key.equals( other.key ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "Entity [key=" + key + ", properties=" + properties + "]";
    }

}
