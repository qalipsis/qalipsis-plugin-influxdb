package io.qalipsis.plugins.influxdb.poll.converters

import io.qalipsis.plugins.influxdb.poll.InfluxDbPollRecord
import java.util.concurrent.atomic.AtomicLong
import org.influxdb.dto.Query
import org.influxdb.dto.QueryResult

/**
 * Default converter of InfluxDB document.
 *
 * @author Alex Averyanov
 */
internal abstract class AbstractInfluxDbDocumentPollConverter {
    protected fun convert(queries: QueryResult): List<QueryResult.Series> {
            return queries.results[0].series
    }
}


