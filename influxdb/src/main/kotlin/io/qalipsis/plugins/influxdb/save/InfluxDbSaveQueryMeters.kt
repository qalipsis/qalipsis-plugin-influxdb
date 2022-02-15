package io.qalipsis.plugins.influxdb.save

import java.time.Duration

/**
 * Meters of the performed query.
 *
 * @property savedPoints count of saved points
 * @property failedPoints count of failed points
 * @property timeToResult time to until the confirmation of the response (successful or failed)
 *
 * @author Palina Bril
 */
data class InfluxDbSaveQueryMeters(
    val savedPoints: Int,
    val failedPoints: Int,
    val timeToResult: Duration
)
