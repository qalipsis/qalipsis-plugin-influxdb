package io.qalipsis.plugins.influxdb.poll

import io.aerisconsulting.catadioptre.KTestable
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.api.sync.Latch
import java.time.Duration
import javax.validation.constraints.NotBlank
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.influxdb.InfluxDB

/**
 * Database reader based upon
 *
 * @property pollStatement statement to execute
 * @property pollDelay duration between the end of a poll and the start of the next one
 * @property resultsChannelFactory factory to create the channel containing the received results sets
 * @property running running state of the reader
 * @property pollingJob instance of the background job polling data from the database
 * @property eventsLogger the logger for events to track what happens during save query execution.
 * @property meterRegistry registry for the meters.
 *
 * @author Alex Averyanov
 */
internal class InfluxDbIterativeReader(
    private val coroutineScope: CoroutineScope,
    private val connectionConfiguration: InfluxDbPollStepConnectionImpl,
    private val clientFactory: () -> InfluxDB,
    private val pollStatement: PollStatement,
    private val pollDelay: Duration,
    private val query: String,
    private val bindParameters: Map<@NotBlank String, Any>,
    private val resultsChannelFactory: () -> Channel<InfluxDbQueryResult> = { Channel(Channel.UNLIMITED) },
    private val eventsLogger: EventsLogger?,
    private val meterRegistry: MeterRegistry?
) : DatasourceIterativeReader<InfluxDbQueryResult> {

    private val eventPrefix = "influxdb.poll"

    private val meterPrefix = "influxdb-poll"

    private var running = false

    private lateinit var client: InfluxDB

    private lateinit var pollingJob: Job

    private lateinit var resultsChannel: Channel<InfluxDbQueryResult>

    private lateinit var context: StepStartStopContext

    private var recordsCount: Counter? = null

    private var timeToResponse: Timer? = null

    private var successCounter: Counter? = null

    private var failureCounter: Counter? = null

    override fun start(context: StepStartStopContext) {
        log.debug { "Starting the step with the context $context" }
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            recordsCount = counter("$meterPrefix-received-records", tags)
            timeToResponse = timer("$meterPrefix-time-to-response", tags)
            successCounter = counter("$meterPrefix-successes", tags)
            failureCounter = counter("$meterPrefix-failures", tags)
        }
        this.context = context
        init()
        pollingJob = coroutineScope.launch {
            log.debug { "Polling job just started for context $context" }
            try {
                while (running) {
                    poll(client)
                    if (running) {
                        delay(pollDelay.toMillis())
                    }
                }
                log.debug { "Polling job just completed for context $context" }
            } finally {
                resultsChannel.cancel()
            }
        }
        running = true
    }

    override fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(recordsCount!!)
            remove(timeToResponse!!)
            remove(successCounter!!)
            remove(failureCounter!!)
            recordsCount = null
            timeToResponse = null
            successCounter = null
            failureCounter = null
        }
        running = false
        runCatching {
            runBlocking {
                pollingJob.cancelAndJoin()
            }
        }
        runCatching {
            client.close()
        }
        resultsChannel.cancel()
        pollStatement.reset()
    }

    @KTestable
    fun init() {
        resultsChannel = resultsChannelFactory()
        client = clientFactory()
    }

    private suspend fun poll(client: InfluxDB) {
        try {
            val latch = Latch(true)
            eventsLogger?.trace("$eventPrefix.polling", tags = context.toEventTags())
            val requestStart = System.nanoTime()

            client.query(pollStatement.convertQueryForNextPoll(query.toString(), connectionConfiguration, bindParameters),  {
                val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                coroutineScope.launch {
                    eventsLogger?.info(
                        "$eventPrefix.successful-response",
                        arrayOf(duration, it.results[0].series),
                        tags = context.toEventTags()
                    )
                    successCounter?.increment()
                    recordsCount?.increment(it.results[0].series.size.toDouble())
                    if (it.results[0].series != null) {
                        log.debug { "Received ${it.results[0].series.size} documents" }
                        resultsChannel.send(
                            InfluxDbQueryResult(
                                queryResult = it,
                                meters = InfluxDbQueryMeters(it.results[0].series.size, duration)
                            )
                        )
                        pollStatement.saveTieBreakerValueForNextPoll(it)
                    } else {
                        log.debug { "No new document was received" }
                    }
                    latch.cancel()
                }
            },  {
                val duration = Duration.ofNanos(System.nanoTime() - requestStart)
                eventsLogger?.warn(
                    "$eventPrefix.failure",
                    arrayOf(it, duration),
                    tags = context.toEventTags()
                )
                failureCounter?.increment()

                latch.cancel()
            })
            latch.await()
        } catch (e: InterruptedException) {
            // The exception is ignored.
        } catch (e: CancellationException) {
            // The exception is ignored.
        } catch (e: Exception) {
            log.error(e) { e.message }
        }
    }
    private companion object {
        @JvmStatic
        val log = logger()
    }

    override suspend fun hasNext(): Boolean = running

    override suspend fun next(): InfluxDbQueryResult = resultsChannel!!.receive()
}
