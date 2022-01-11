package io.qalipsis.plugins.mondodb.converters

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.influxdb.converters.InfluxDbDefaultConverter
import io.qalipsis.plugins.influxdb.poll.InfluxDbQueryResult
import io.qalipsis.plugins.influxdb.poll.InfluxDbPollRecord
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a batch of InfluxDb documents and forwards each of
 * them converted to a [InfluxDbPollRecord].
 *
 * @author Alex Averyanov
 */
internal class InfluxDbDocumentPollSingleConverter(
    private val databaseName: String,
    private val collectionName: String,
) : DatasourceObjectConverter<InfluxDbQueryResult, InfluxDbPollRecord>, InfluxDbDefaultConverter() {

    override suspend fun supply(offset: AtomicLong, value: InfluxDbQueryResult, output: StepOutput<InfluxDbPollRecord>) {
        tryAndLogOrNull(log) {
            convert(offset, value.documents, databaseName, collectionName).forEach { output.send(it) }
        }
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
