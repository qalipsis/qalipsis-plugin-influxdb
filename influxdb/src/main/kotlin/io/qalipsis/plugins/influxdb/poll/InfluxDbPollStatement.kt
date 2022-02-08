package io.qalipsis.plugins.influxdb.poll

import com.influxdb.client.domain.Query
import com.influxdb.query.FluxRecord
import java.time.Instant
import javax.validation.constraints.NotBlank

/**
 * InfluxDb statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @property tieBreaker - tie breaker instant
 * @author Alex Averyanov
 */
internal class InfluxDbPollStatement : PollStatement {

    private var tieBreaker: Any? = null

    override fun saveTieBreakerValueForNextPoll(queryResult: FluxRecord) {
        val latestTimeStamp = (queryResult.time)
        if (tieBreaker != null) {
            if (latestTimeStamp!!.isAfter(tieBreaker as Instant?)) {
                tieBreaker = latestTimeStamp
            }
        } else {
            tieBreaker = latestTimeStamp
        }
    }

    private fun bindParamsToBuilder(queryBuilder: String, bindParameters: Map<@NotBlank String, Any>): StringBuilder {
        val queryStringBuilder = StringBuilder(queryBuilder)
        bindParameters.forEach { (k, v) ->
            queryStringBuilder.append(" |> filter(fn: (r) => r.$k == \"$v\") ")
        }
        return queryStringBuilder
    }

    override fun convertQueryForNextPoll(
        queryString: String, connectionConfiguration: InfluxDbPollStepConnectionImpl,
        bindParameters: Map<@NotBlank String, Any>
    ): Query {
        var queryStringBuilder = StringBuilder(queryString)
        if (tieBreaker != null) {
            queryStringBuilder.append(" |> range(start: $tieBreaker)")
                .append(" |> filter(fn: (r) => r._time >= $tieBreaker) ")
        } else {
            queryStringBuilder.append(" |> range(start: 0) ")
        }
        if (bindParameters.isNotEmpty()) {
            queryStringBuilder = StringBuilder(bindParamsToBuilder(queryStringBuilder.toString(), bindParameters))
        }
        return Query().query(queryStringBuilder.toString())
    }

    override fun reset() {
        tieBreaker = null
    }
}
