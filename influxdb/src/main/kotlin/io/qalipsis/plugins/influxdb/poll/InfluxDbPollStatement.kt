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
internal class InfluxDbPollStatement(
    val tieBreakerName: String
): PollStatement {

     private var tieBreaker: Any? = null

    override fun saveTieBreakerValueForNextPoll(queryResult: FluxRecord) {
        var maxTimeStamp: Instant? = null
        val latestTimeStamp = (queryResult.time)
        if(maxTimeStamp == null) {
            maxTimeStamp = latestTimeStamp
        } else if(latestTimeStamp!!.isAfter(maxTimeStamp)) {
            maxTimeStamp = latestTimeStamp
        }

        tieBreaker = maxTimeStamp
    }

    private fun bindParamsToBuilder(queryBuilder: String, bindParameters: Map<@NotBlank String, Any>) : StringBuilder {
        val queryStringBuilder = StringBuilder(queryBuilder)
        bindParameters.forEach{ (k, v) ->
            queryStringBuilder.append("|> filter(fn: (r) => r.$k == $v)")
        }
        return queryStringBuilder
    }
    override fun convertQueryForNextPoll(queryString: String, connectionConfiguration: InfluxDbPollStepConnectionImpl, bindParameters: Map<@NotBlank String, Any>): Query {
        var queryStringBuilder = StringBuilder()
        if(bindParameters.isNotEmpty()) {
            queryStringBuilder = StringBuilder(bindParamsToBuilder(queryString, bindParameters))
        }
        return return if(tieBreaker != null) {
            queryStringBuilder.append("|> range(start: $tieBreaker)")
            Query().query(queryStringBuilder.toString())
        } else {
            Query().query(queryStringBuilder.toString())
        }
        /*val query = Query()
        query.query = queryString
        query.params = bindParameters
        return query*/
    }


    /*fun convertQueryForNextPoll1(queryString: String, connectionConfiguration: InfluxDbPollStepConnectionImpl, bindParameters: Map<@NotBlank String, Any>) {
        val queryStringBuilder = StringBuilder(queryString)
        if(bindParameters.isEmpty()) {
            if(tieBreaker != null) {

            }
        }

        /* if(tieBreaker != null) {
            var queryBuilder: BoundParameterQuery.QueryBuilder = BoundParameterQuery.QueryBuilder.newQuery("$queryStringBuilder AND $tieBreakerName >= '$tieBreaker'")
            queryBuilder.forDatabase(connectionConfiguration.bucket)
            queryBuilder = bindParamsToBuilder(queryBuilder, bindParameters)
            queryBuilder.create()
        } else {
            var queryBuilder: BoundParameterQuery.QueryBuilder = BoundParameterQuery.QueryBuilder.newQuery(queryString)
            queryBuilder.forDatabase(connectionConfiguration.bucket)
            queryBuilder = bindParamsToBuilder(queryBuilder, bindParameters)
            queryBuilder.create()
        }*/
    }*/

    override fun reset() {
        tieBreaker = null
    }
}
