package io.qalipsis.plugins.influxdb.poll

import org.bson.Document
import org.bson.conversions.Bson
import org.influxdb.dto.Query

/**
 * statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @author Alex Averyanov
 */
internal interface PollStatement {

    /**
     * Saves actual tie-breaker value from previous poll. A value will be used to compose next query.
     */
    fun saveTieBreakerValueForNextPoll(query: Query)

    /**
     * Resets the instance into the initial state to be ready for a new poll sequence starting from scratch.
     */
    fun reset()

}