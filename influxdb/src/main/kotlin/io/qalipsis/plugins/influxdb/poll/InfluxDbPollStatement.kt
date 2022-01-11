package io.qalipsis.plugins.influxdb.poll

import org.influxdb.dto.Query

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
    val databaseName: String,
    val collectionName: String,
    private val findClause: Bson,
    private val tieBreakerName: String,
) : PollStatement {

    private var tieBreaker: BsonValue? = null

    private val findClauseAsBson = findClause.toBsonDocument()

    override fun saveTieBreakerValueForNextPoll(query: Query) {
        tieBreaker = query.toBsonDocument()[tieBreakerName]
    }

    override fun reset() {
        tieBreaker = null
    }
}
