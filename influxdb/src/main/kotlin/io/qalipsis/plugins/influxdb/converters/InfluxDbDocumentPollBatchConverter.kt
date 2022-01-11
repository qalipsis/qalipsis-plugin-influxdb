package io.qalipsis.plugins.mondodb.converters

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.influxdb.converters.InfluxDbDefaultConverter
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
    private val databaseName: String,
    private val collectionName: String,
) : DatasourceObjectConverter<InfluxDbQueryResult, InfluxDbPollResults>, InfluxDbDefaultConverter() {

    override suspend fun supply(
        offset: AtomicLong,
        value: InfluxDbQueryResult,
        output: StepOutput<InfluxDbPollResults>
    ) {
        tryAndLogOrNull(log) {
            output.send(
                InfluxDbPollResults(
                    results = convert(offset, value.documents, databaseName, collectionName),
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
