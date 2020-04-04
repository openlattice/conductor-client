package com.openlattice.datastore.configuration

import com.amazonaws.services.s3.model.StorageClass
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration

const val BUCKET_NAME = "bucketName"
const val REGION_NAME = "regionName"
const val TIME_TO_LIVE = "timeToLive"
const val ACCESS_KEY_ID = "accessKeyId"
const val SECRET_ACCESS_KEY = "secretAccessKey"
const val STORES = "stores"

@ReloadableConfiguration(uri = "datastore.yaml")
data class DatastoreConfiguration(
        @JsonProperty(BUCKET_NAME) val bucketName: String,
        @JsonProperty(REGION_NAME) val regionName: String,
        @JsonProperty(TIME_TO_LIVE) val timeToLive: Long,
        @JsonProperty(ACCESS_KEY_ID) val accessKeyId: String,
        @JsonProperty(SECRET_ACCESS_KEY) val secretAccessKey: String,
        @JsonProperty(STORES) val stores: Map<StorageClass,List<S3StorageConfiguration>> = mapOf()
) : Configuration {

    companion object {
        @JvmStatic
        @get:JvmName("key")
        val key = SimpleConfigurationKey("datastore.yaml")
    }

    override fun getKey(): ConfigurationKey {
        return DatastoreConfiguration.key
    }
}