package io.qalipsis.plugins.influxdb.save

/**
 * Wrapper for the result of save points procedure in InfluxDB.
 *
 * @property input the data to save in InfluxDB
 * @property meters meters of the save step
 *
 */
internal class InfluxDBSaveResult<I>(
    val input: I,
    val meters: InfluxDbSaveQueryMeters
)
