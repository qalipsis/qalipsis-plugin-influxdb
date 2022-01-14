package io.qalipsis.plugins.influxdb.poll

import org.influxdb.dto.QueryResult

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
    val results: List<QueryResult.Series>,
    val meters: InfluxDbQueryMeters
) : Iterable<QueryResult.Series> {

    override fun iterator(): Iterator<QueryResult.Series> {
        return results.iterator()
    }
}
