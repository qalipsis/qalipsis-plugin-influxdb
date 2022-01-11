package io.qalipsis.plugins.influxdb.poll

import org.influxdb.dto.QueryResult


/**
 * A wrapper for meters and documents.
 *
 * @property documents result of search query procedure in InfluxDb
 * @property meters meters of the query
 *
 * @author Alex Averyanov
 */
class InfluxDbQueryResult(
    val documents: List<QueryResult>,
    val meters: InfluxDbQueryMeters
)
