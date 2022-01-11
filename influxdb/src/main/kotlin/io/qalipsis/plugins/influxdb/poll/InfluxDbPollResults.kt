package io.qalipsis.plugins.influxdb.poll

/**
 * Wrapper for the result of poll in InfluxDb.
 *
 *
 * @property results list of InfluxDb records.
 * @property meters of the poll step.
 *
 * @author Alex Averyanov
 */

data class InfluxDbPollResults(
    val results: List<InfluxDbPollRecord>,
    val meters: InfluxDbQueryMeters
) : Iterable<InfluxDbPollRecord> {

    override fun iterator(): Iterator<InfluxDbPollRecord> {
        return results.iterator()
    }
}
