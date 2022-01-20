package io.qalipsis.plugins.influxdb.poll

import java.time.Instant
import javax.validation.constraints.NotBlank
import org.influxdb.dto.BoundParameterQuery
import org.influxdb.dto.Query
import org.influxdb.dto.QueryResult

/**
 * InfluxDb statement for polling, integrating the ability to be internally modified when a tie-breaker is set.
 *
 * @property tieBreaker - tie breaker instant
 * @author Alex Averyanov
 */
internal class InfluxDbPollStatement(
    val tieBreakerName: String
): PollStatement {

     private var tieBreaker: Any? = null

    override fun saveTieBreakerValueForNextPoll(queryResult: QueryResult) {
        var maxTimeStamp: Instant? = null
        for(result in queryResult.results) {
            for (series in result.series) {
                for(value in series.values) {
                    val latestTimeStamp = Instant.parse(value[0].toString())
                    if(maxTimeStamp == null) {
                        maxTimeStamp = latestTimeStamp
                    } else if(latestTimeStamp.isAfter(maxTimeStamp)) {
                        maxTimeStamp = latestTimeStamp
                    }
                }
            }
        }
        tieBreaker = maxTimeStamp
    }

    private fun bindParamsToBuilder(queryBuilder: BoundParameterQuery.QueryBuilder, bindParameters: Map<@NotBlank String, Any>) : BoundParameterQuery.QueryBuilder {
        bindParameters.forEach { (k, v) ->
            queryBuilder.bind(k , v)
        }
        return queryBuilder
    }

    override fun convertQueryForNextPoll(queryString: String, connectionConfiguration: InfluxDbPollStepConnectionImpl, bindParameters: Map<@NotBlank String, Any>): Query {
        val queryStringBuilder = StringBuilder(queryString)
        if(bindParameters.isEmpty()) {
            queryStringBuilder.append(" WHERE ")
        }

        return if(tieBreaker != null) {
            var queryBuilder: BoundParameterQuery.QueryBuilder = BoundParameterQuery.QueryBuilder.newQuery("$queryStringBuilder AND $tieBreakerName >= '$tieBreaker'")
            queryBuilder.forDatabase(connectionConfiguration.database)
            queryBuilder = bindParamsToBuilder(queryBuilder, bindParameters)
            queryBuilder.create()
        } else {
            var queryBuilder: BoundParameterQuery.QueryBuilder = BoundParameterQuery.QueryBuilder.newQuery(queryString)
            queryBuilder.forDatabase(connectionConfiguration.database)
            queryBuilder = bindParamsToBuilder(queryBuilder, bindParameters)
            queryBuilder.create()
        }
    }

    override fun reset() {
        tieBreaker = null
    }
}
