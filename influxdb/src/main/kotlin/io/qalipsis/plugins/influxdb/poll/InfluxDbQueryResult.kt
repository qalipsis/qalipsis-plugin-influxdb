package io.qalipsis.plugins.influxdb.poll

import org.influxdb.dto.QueryResult


/**
 * A wrapper for meters and documents.
 *
 * @property queryResult result of search query procedure in InfluxDb
 * @property meters meters of the query
 *
 * @author Alex Averyanov
 */
class InfluxDbQueryResult(
    val queryResult: QueryResult,
    val meters: InfluxDbQueryMeters
)
