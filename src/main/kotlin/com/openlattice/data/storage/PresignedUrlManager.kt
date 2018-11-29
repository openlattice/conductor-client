package com.openlattice.data.storage

import java.net.URL
import java.util.*

interface PresignedUrlManager {
    fun getPresignedUrls(keys: List<Any>): List<URL>

    fun getPresignedUrl(key: Any, expiration: Date): URL
}