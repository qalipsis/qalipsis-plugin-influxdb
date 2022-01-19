package io.qalipsis.plugins.influxdb.poll

import java.time.Instant
import javax.validation.constraints.NotBlank
import org.influxdb.dto.Query
import org.influxdb.dto.QueryResult

/**
 * statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Alex Averyanov
 */
internal interface PollStatement {
     var tieBreaker: Instant?
    /**
     * Saves actual tie-breaker value from previous poll. A value will be used to compose next query.
     */
    fun saveTieBreakerValueForNextPoll(query: QueryResult)

    /**
     * Changes the query following the first when the tie-breaker is already known
     */
    fun convertQueryForNextPoll(queryString: String, connectionConfiguration: InfluxDbPollStepConnectionImpl, bindParameters: MutableMap<@NotBlank String, Any>): Query

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()

}