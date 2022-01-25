package io.qalipsis.plugins.mondodb.converters

import com.influxdb.query.FluxRecord
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.influxdb.poll.InfluxDbPollResults
import io.qalipsis.plugins.influxdb.poll.InfluxDbQueryResult
import java.util.concurrent.atomic.AtomicLong
/**
 * Implementation of [DatasourceObjectConverter], that reads a batch of InfluxDb documents and forwards it
 * as a list of [InfluxDBSearchResults].
 *
 * @author Alex Averyanov
 */
internal class InfluxDbDocumentPollBatchConverter(
) :  DatasourceObjectConverter<InfluxDbQueryResult, InfluxDbPollResults>{

    private fun convert(queries: List<FluxRecord>): List<FluxRecord> {
        return queries
    }
    override suspend fun supply(
        offset: AtomicLong,
        value: InfluxDbQueryResult,
        output: StepOutput<InfluxDbPollResults>
    ) {
        tryAndLogOrNull(log) {
            output.send(
                InfluxDbPollResults(
                    results = convert(value.queryResults),
                    meters = value.meters
                )
            )
        }
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
