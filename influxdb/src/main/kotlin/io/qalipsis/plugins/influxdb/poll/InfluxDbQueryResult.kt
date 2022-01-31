package io.qalipsis.plugins.influxdb.poll

import com.influxdb.query.FluxRecord

/**
 * A wrapper for meters and documents.
 *
 * @property queryResults result of search query procedure in InfluxDb
 * @property meters meters of the query
 *
 * @author Alex Averyanov
 */
internal class InfluxDbQueryResult(
    val queryResults: List<FluxRecord>,
    val meters: InfluxDbQueryMeters
)
