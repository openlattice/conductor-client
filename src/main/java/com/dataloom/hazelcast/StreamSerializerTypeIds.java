package com.dataloom.hazelcast;

/**
 * Simplifies management of type ids within Hazelcast for serialization. Can be re-ordered safely unless doing hot upgrade.
 * <b>NOTE: Leave first entry in place</b> 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt; 
 *
 */
public enum StreamSerializerTypeIds {
    /**
     * Move this one, break everything.
     */
    STREAM_SERIALIZER_IDS_MUST_BE_POSTIVE,
    RUNNABLE,
    CALLABLE,
    CONDUCTOR_CALL,
    EMPLOYEE,
    ENTITY_SET,
    FULL_QUALIFIED_NAME,
    QUERY_RESULT,
    TYPE_PK

}
