package io.qalipsis.plugins.influxdb.save

import com.influxdb.client.write.Point
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope

/**
 * [StepSpecificationConverter] from [InfluxDbSaveStepSpecificationImpl] to [InfluxDbSaveStep]
 * to use the Save API.
 *
 */
@StepConverter
internal class InfluxDbSaveStepSpecificationConverter(
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger
) : StepSpecificationConverter<InfluxDbSaveStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is InfluxDbSaveStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<InfluxDbSaveStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name

        @Suppress("UNCHECKED_CAST")
        val step = InfluxDbSaveStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            influxDbSavePointClient = InfluxDbSavePointClientImpl(
                ioCoroutineScope,
                spec.clientBuilder,
                eventsLogger = supplyIf(spec.monitoringConfig.events) { eventsLogger },
                meterRegistry = supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            ),
            bucketName = spec.pointConfig.bucket as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            orgName = spec.pointConfig.organization as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            pointsFactory = spec.pointConfig.points as suspend (ctx: StepContext<*, *>, input: I) -> List<Point>,
        )
        creationContext.createdStep(step)
    }
}
