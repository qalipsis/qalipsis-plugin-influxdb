package io.qalipsis.plugins.influxdb.poll

import org.bson.Document
import org.influxdb.dto.Query
import org.influxdb.dto.QueryResult

/**
 * Qalipsis representation of a fetched InfluxDb document.
 *
 * @author Alex Averyanov
 *
 * @property value [Map] of value from [Document]
 * @property id special identifier from db
 * @property source name of db and collection (dbname.colname example) from data were received
 * @property offset record offset as provided by InfluxDb
 * @property receivingInstant received timestamp as provided by InfluxDb
 */
data class InfluxDbPollRecord(
    val value: Map<String, Any?>,
    val id: Any,
    val source: String,
    val offset: Long,
    val receivingInstant: Long = System.currentTimeMillis(),
) {
    internal constructor(
        offset: Long, record: QueryResult, database: String, collection: String, idField: String? = "_id"
    ) : this(
        value = record.toMap(),
        id = record.getObjectId(idField),
        source = "$database.$collection",
        offset = offset
    )
}