package io.qalipsis.plugins.influxdb.poll

import com.influxdb.client.domain.Query
import com.influxdb.query.FluxRecord
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
     * By default sort with desc = false (ascending order). If you need descending order - desc should be true.
     */
    fun convertQueryForNextPoll(
        queryString: String,
        connectionConfiguration: InfluxDbPollStepConnectionImpl,
        bindParameters: Map<@NotBlank String, Any>,
        sortField: List<String>,
        desc: Boolean
    ): Query

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()

}