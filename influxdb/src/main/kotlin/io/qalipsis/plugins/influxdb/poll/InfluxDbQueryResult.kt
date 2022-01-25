package io.qalipsis.plugins.influxdb.poll

import com.influxdb.query.FluxRecord

/**
 * A wrapper for meters and documents.
 *
 * @property queryResult result of search query procedure in InfluxDb
 * @property meters meters of the query
 *
 * @author Alex Averyanov
 */
class InfluxDbQueryResult(
    val queryResults: List<FluxRecord>,
    val meters: InfluxDbQueryMeters
)
