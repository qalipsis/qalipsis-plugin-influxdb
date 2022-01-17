package io.qalipsis.plugins.influxdb.poll

import java.time.Instant
import java.util.Properties
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
) : PollStatement {
    override var tieBreaker: Instant?
        get() = this.tieBreaker
        set(value) {tieBreaker = value}

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

    private fun bindParamsToBuilder(queryBuilder: BoundParameterQuery.QueryBuilder, bindParameters: Properties) : BoundParameterQuery.QueryBuilder {
        bindParameters.forEach { k, v ->
            queryBuilder.bind(k as String?,v)
        }
        return queryBuilder
    }

    override fun convertQueryForNextPoll(queryString: String, connectionConfiguration: InfluxDbPollStepConnectionImpl, bindParameters: Properties): Query {
        return if(tieBreaker != null) {
            var queryBuilder: BoundParameterQuery.QueryBuilder = BoundParameterQuery.QueryBuilder.newQuery(queryString + "and time > $tieBreaker")
            queryBuilder = bindParamsToBuilder(queryBuilder, bindParameters)
            queryBuilder.forDatabase(connectionConfiguration.database).create()
        } else {
            var queryBuilder: BoundParameterQuery.QueryBuilder = BoundParameterQuery.QueryBuilder.newQuery(queryString)
            queryBuilder = bindParamsToBuilder(queryBuilder, bindParameters)
            queryBuilder.forDatabase(connectionConfiguration.database).create()
        }
    }

    override fun reset() {
        tieBreaker = null
    }
}
