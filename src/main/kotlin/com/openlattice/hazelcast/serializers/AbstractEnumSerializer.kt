package com.openlattice.hazelcast.serializers

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import org.apache.commons.lang.math.RandomUtils

abstract class AbstractEnumSerializer<T : Enum<T>> : TestableSelfRegisteringStreamSerializer<Enum<T>> {
    protected val enumArray = enumCache.getOrPut(clazz) { this.clazz.enumConstants as Array<Enum<*>> } as Array<Enum<T>>

    companion object {
        private val enumCache: MutableMap<Class<*>, Array<Enum<*>>> = mutableMapOf()

        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: Enum<*>) {
            out.writeInt(`object`.ordinal)
        }

        @JvmStatic
        fun deserialize(targetClass: Class<*>, `in`: ObjectDataInput): Enum<*> {
            val ord = `in`.readInt()
            return enumCache.getValue(targetClass)[ord]
        }
    }

    override fun write(out: ObjectDataOutput, `object`: Enum<T>) {
        return serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): Enum<T> {
        return enumArray[`in`.readInt()]
    }

    override fun generateTestValue(): Enum<T> {
        return enumArray[RandomUtils.nextInt(enumArray.size)]
    }
}