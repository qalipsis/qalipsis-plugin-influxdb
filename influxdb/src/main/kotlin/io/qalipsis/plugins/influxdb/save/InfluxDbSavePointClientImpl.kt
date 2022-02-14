package io.qalipsis.plugins.influxdb.save

import com.influxdb.client.InfluxDBClient
import com.influxdb.client.write.Point
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.tryAndLog
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.sync.Slot
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Implementation of [InfluxDbSavePointClient].
 * Client to query to InfluxDB.
 *
 * @property clientBuilder supplier for the InfluxDb client.
 * @property eventsLogger the logger for events to track what happens during save step execution.
 * @property meterRegistry registry for the meters.
 *
 */
internal class InfluxDbSavePointClientImpl(
    private val ioCoroutineScope: CoroutineScope,
    private val clientBuilder: () -> InfluxDBClient,
    private var eventsLogger: EventsLogger?,
    private val meterRegistry: MeterRegistry?
) : InfluxDbSavePointClient {

    private lateinit var client: InfluxDBClient

    private val eventPrefix = "influxdb.save"

    private val meterPrefix = "influxdb-save"

    private var pointsCounter: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    override suspend fun start(context: StepStartStopContext) {
        client = clientBuilder()
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            pointsCounter = counter("$meterPrefix-saving-points", tags)
            timeToResponse = timer("$meterPrefix-time-to-response", tags)
            successCounter = counter("$meterPrefix-successes", tags)
            failureCounter = counter("$meterPrefix-failures", tags)
        }
    }

    override suspend fun execute(
        bucketName: String,
        orgName: String,
        points: List<Point>,
        contextEventTags: Map<String, String>
    ): InfluxDbSaveQueryMeters {
        val result = Slot<Result<InfluxDbSaveQueryMeters>>()
        eventsLogger?.debug("$eventPrefix.saving-points", points.size, tags = contextEventTags)
        var successSavedPoints = 0
        var failedSavedPoints = 0
        pointsCounter?.increment(points.size.toDouble())
        val requestStart = System.nanoTime()
        points.forEach {
            try {
                client.writeApiBlocking.writePoint(bucketName, orgName, it)
                successSavedPoints++
            } catch (e: Exception) {
                failedSavedPoints++
                ioCoroutineScope.launch {
                    result.set(Result.failure(e))
                }
            }
        }
        val timeToResponse = System.nanoTime() - requestStart
        eventsLogger?.info(
            "${eventPrefix}.time-to-response",
            Duration.ofMillis(timeToResponse),
            tags = contextEventTags
        )
        eventsLogger?.info("${eventPrefix}.successes", successSavedPoints, tags = contextEventTags)
        if (failedSavedPoints > 0) {
            eventsLogger?.info("${eventPrefix}.failures", failedSavedPoints, tags = contextEventTags)
            failureCounter?.increment(failedSavedPoints.toDouble())
        }

        val influxDbSaveStepMeters = InfluxDbSaveQueryMeters(
            failedPoints = failedSavedPoints,
            timeToResult = Duration.ofMillis(timeToResponse),
            savedPoints = successSavedPoints
        )
        successCounter?.increment(influxDbSaveStepMeters.savedPoints.toDouble())
        this.timeToResponse?.record(timeToResponse, TimeUnit.NANOSECONDS)
        ioCoroutineScope.launch {
            result.set(Result.success(influxDbSaveStepMeters))
        }
        return result.get().getOrThrow()
    }

    override suspend fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(pointsCounter!!)
            remove(timeToResponse!!)
            remove(successCounter!!)
            remove(failureCounter!!)
            pointsCounter = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
        }
        tryAndLog(log) {
            client.close()
        }
    }

    companion object {
        @JvmStatic
        private val log = logger()
    }
}
