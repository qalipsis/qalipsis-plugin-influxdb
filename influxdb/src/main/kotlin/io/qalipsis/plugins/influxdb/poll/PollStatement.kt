package io.qalipsis.plugins.influxdb.poll

import com.influxdb.client.domain.Query
import com.influxdb.query.FluxRecord
import java.time.Instant
import javax.validation.constraints.NotBlank


/**
 * statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Alex Averyanov
 */
internal interface PollStatement {
    /**
     * Saves actual tie-breaker value from previous poll. A value will be used to compose next query.
     */
    fun saveTieBreakerValueForNextPoll(query: FluxRecord)

    /**
     * Changes the query following the first when the tie-breaker is already known
     */
    fun convertQueryForNextPoll(queryString: String, connectionConfiguration: InfluxDbPollStepConnectionImpl, bindParameters: Map<@NotBlank String, Any>): Query

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()

}