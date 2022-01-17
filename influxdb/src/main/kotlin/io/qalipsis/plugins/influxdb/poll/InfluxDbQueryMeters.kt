package io.qalipsis.plugins.influxdb.poll

import java.time.Duration

/**
 * Meters of the performed query.
 *
 * @property fetchedRecords count of received records
 * @property timeToResult time to until the complete successful response
 *
 * @author Alex Averyanov
 */
data class InfluxDbQueryMeters(
    val fetchedRecords: Int,
    val timeToResult: Duration
)
