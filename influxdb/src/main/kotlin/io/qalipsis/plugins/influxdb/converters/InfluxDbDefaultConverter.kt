package io.qalipsis.plugins.influxdb.converters

import io.qalipsis.plugins.influxdb.poll.InfluxDbPollRecord
import java.util.concurrent.atomic.AtomicLong
import org.influxdb.dto.Query

/**
 * Default converter of InfluxDB document.
 *
 * @author Alex Averyanov
 */
internal abstract class InfluxDbDefaultConverter {
    protected fun convert(offset: AtomicLong, queries: List<Query>, databaseName: String, collectionName: String): List<InfluxDbPollRecord> {
        return queries.map { query ->
            InfluxDbPollRecord(
                offset = offset.getAndIncrement(),
                record = query,
                database = databaseName,
                collection = collectionName
            )
        }
    }
}

