package com.openlattice.data.storage

import com.amazonaws.HttpMethod
import java.net.URL
import java.util.*


interface ByteBlobDataManager {
    fun putObject(s3Key: String, data: ByteArray, contentType: String)

    fun deleteObject(s3Key: String)

    fun getObjects(keys: Collection<Any>): List<Any>

    fun getObjectsAsMap(keys: Collection<Any>): Map<Any, Any>

    fun getPresignedUrl(key: Any, expiration: Date, httpMethod: HttpMethod = HttpMethod.GET, contentType: Optional<String>): URL
  
    fun getPresignedUrls(keys: Collection<Any>): List<URL>

    fun getPresignedUrlsAsMap(keys: Collection<Any>): Map<Any, URL>
  
    fun deleteObjects(s3Keys: List<String>)
}