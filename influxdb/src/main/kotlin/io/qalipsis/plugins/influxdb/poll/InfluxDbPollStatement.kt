package io.qalipsis.plugins.influxdb.poll

import java.text.SimpleDateFormat
import java.time.Instant
import org.influxdb.dto.QueryResult


/**
 * InfluxDb statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @property databaseName - a name of a database
 * @property collectionName - a name of a collection
 * @property findClause - initial find clause for the first request
 * @property sortClauseValues - a map with field name as a key and ordering as a value
 * @property tieBreakerName - tie breaker name
 * @author Alex Averyanov
 */
internal class InfluxDbPollStatement(
    override var tieBreaker: Instant?
) : PollStatement {

    override fun saveTieBreakerValueForNextPoll(queryResult: QueryResult) {
        var maxTimeStamp: Instant? = null
        for(result in queryResult.results) {
            for (series in result.series) {
                for(value in series.values) {
                    val latestTimeStamp = Instant.parse(value[0].toString())
                    if(latestTimeStamp.isAfter(maxTimeStamp)) {
                        maxTimeStamp = latestTimeStamp
                    }
                }
            }
        }
        tieBreaker = maxTimeStamp
    }

    override fun reset() {
        tieBreaker = null
    }
}
