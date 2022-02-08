package io.qalipsis.plugins.influxdb.poll

import com.influxdb.client.InfluxDBClientFactory
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor

import io.qalipsis.plugins.mondodb.converters.InfluxDbDocumentPollBatchConverter
import jakarta.inject.Named

import kotlinx.coroutines.CoroutineScope

/**
 * [StepSpecificationConverter] from [InfluxDbPollStepSpecificationImpl] to [InfluxDbIterativeReader] for a data source.
 *
 * @author Alexander Sosnovsky
 */
@StepConverter
internal class InfluxDbPollStepSpecificationConverter(
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger,
    @Named(Executors.IO_EXECUTOR_NAME) private val coroutineScope: CoroutineScope
) : StepSpecificationConverter<InfluxDbPollStepSpecificationImpl> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is InfluxDbPollStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<InfluxDbPollStepSpecificationImpl>) {
        val spec = creationContext.stepSpecification
        val pollStatement = buildPollStatement()
        val stepId = spec.name

        val reader = InfluxDbIterativeReader(
            clientFactory = {
                InfluxDBClientFactory.create(
                    spec.connectionConfiguration.url,
                    spec.connectionConfiguration.user.toCharArray(),
                    spec.connectionConfiguration.password
                )
            },
            coroutineScope = coroutineScope,
            connectionConfiguration = spec.connectionConfiguration,
            pollStatement = pollStatement,
            query = spec.query,
            bindParameters = spec.bindParameters,
            pollDelay = spec.pollPeriod,
            eventsLogger = supplyIf(spec.monitoringConfiguration.events) { eventsLogger },
            meterRegistry = supplyIf(spec.monitoringConfiguration.meters) { meterRegistry }
        )

        val converter = buildConverter()

        val step = IterativeDatasourceStep(
            stepId,
            reader,
            NoopDatasourceObjectProcessor(),
            converter
        )
        creationContext.createdStep(step)
    }

    fun buildPollStatement(): PollStatement {
        return InfluxDbPollStatement()
    }

    private fun buildConverter(): DatasourceObjectConverter<InfluxDbQueryResult, out Any> {
        return InfluxDbDocumentPollBatchConverter()
    }
}
