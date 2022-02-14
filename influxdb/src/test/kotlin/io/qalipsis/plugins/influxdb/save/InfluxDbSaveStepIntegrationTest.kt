package io.qalipsis.plugins.influxdb.save

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.key
import com.influxdb.Cancellable
import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.InfluxDBClientOptions
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import com.influxdb.query.FluxRecord
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.InfluxDBContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.time.Period
import java.util.concurrent.TimeUnit
import kotlin.math.pow

@Testcontainers
@WithMockk
internal class InfluxDbSaveStepIntegrationTest {

    private lateinit var client: InfluxDBClient
    protected val connectionConfig = InfluxDbSaveStepConnectionImpl()

    val testDispatcherProvider = TestDispatcherProvider()

    @BeforeAll
    fun init() {
        connectionConfig.url = influxDBContainer.url
        connectionConfig.password = "passpasspass"
        connectionConfig.user = "user"
        connectionConfig.org = "testtesttest"
        connectionConfig.bucket = "test"

        client = InfluxDBClientFactory.create(
            InfluxDBClientOptions.builder()
                .url(connectionConfig.url)
                .authenticate(
                    connectionConfig.user,
                    connectionConfig.password.toCharArray()
                )
                .org(connectionConfig.org)
                .build()
        )
    }

    @AfterAll
    fun shutDown() {
        client.close()
    }

    private val timeToResponse = relaxedMockk<Timer>()

    private val recordsCount = relaxedMockk<Counter>()

    private val failureCounter = relaxedMockk<Counter>()

    private val eventsLogger = relaxedMockk<EventsLogger>()

    @Test
    @Timeout(10)
    fun `should succeed when sending query with single results`() = testDispatcherProvider.run {
        val metersTags = relaxedMockk<Tags>()
        val meterRegistry = relaxedMockk<MeterRegistry> {
            every { counter("influxdb-save-saving-points", refEq(metersTags)) } returns recordsCount
            every { timer("influxdb-save-time-to-response", refEq(metersTags)) } returns timeToResponse
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }
        val countLatch = SuspendedCountLatch(1)
        val results = ArrayList<Map<String, Any>>()
        val point = Point.measurement("temp")
            .addTag("tag1", "first")
            .addField("key1", "val1")
            .time(Instant.now().toEpochMilli() * 1000000, WritePrecision.NS)
        val saveClient = InfluxDbSavePointClientImpl(
            ioCoroutineScope = this,
            clientBuilder = { client },
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )
        val tags: Map<String, String> = emptyMap()

        saveClient.start(startStopContext)

        val resultOfExecute = saveClient.execute(connectionConfig.bucket, connectionConfig.org, listOf(point), tags)

        assertThat(resultOfExecute).isInstanceOf(InfluxDbSaveQueryMeters::class.java).all {
            prop("savedPoints").isEqualTo(1)
            prop("failedPoints").isEqualTo(0)
            prop("failedPoints").isNotNull()
        }

        fetchResult(client, results, countLatch)
        countLatch.await()
        assertThat(results).all {
            hasSize(1)
            index(0).all {
                key("_value").isEqualTo("val1")
            }
        }

        verify {
            eventsLogger.debug("influxdb.save.saving-points", 1, any(), tags = tags)
            timeToResponse.record(more(0L), TimeUnit.NANOSECONDS)
            recordsCount.increment(1.0)
            eventsLogger.info("influxdb.save.time-to-response", any<Duration>(), any(), tags = tags)
            eventsLogger.info("influxdb.save.successes", any<Array<*>>(), any(), tags = tags)
        }
        confirmVerified(timeToResponse, recordsCount, eventsLogger)
    }

    @Test
    @Timeout(10)
    fun `should count failures when sending points with date earlier than retantion policy allows`(): Unit = testDispatcherProvider.run {
        val metersTags = relaxedMockk<Tags>()
        val meterRegistry = relaxedMockk<MeterRegistry> {
            every { counter("influxdb-save-failures", refEq(metersTags)) } returns failureCounter
        }
        val startStopContext = relaxedMockk<StepStartStopContext> {
            every { toMetersTags() } returns metersTags
        }

        val saveClient = InfluxDbSavePointClientImpl(
            ioCoroutineScope = this,
            clientBuilder = { client },
            meterRegistry = meterRegistry,
            eventsLogger = eventsLogger
        )
        val tags: Map<String, String> = emptyMap()
        saveClient.start(startStopContext)

        saveClient.execute(
            connectionConfig.bucket,
            connectionConfig.org,
            listOf(
                Point("smth").addTag("tag2", "second").addField("key2", "value2")
                    .time(Instant.now().minus(Period.ofDays(3)).toEpochMilli() * 1000000, WritePrecision.NS)
            ), // Retention policy is just 2 days
            tags
        )
        verify {
            failureCounter.increment(1.0)
        }
        confirmVerified(failureCounter)
    }

    private fun fetchResult(
        client: InfluxDBClient, results: ArrayList<Map<String, Any>>,
        countLatch: SuspendedCountLatch
    ) {
        client.run {
            queryApi.query("from(bucket: \"test\") |> range(start: 0) |> filter(fn: (r) => r._value == \"val1\") ",
                { _: Cancellable, fluxRecord: FluxRecord ->
                    results.add(fluxRecord.values)
                    countLatch.blockingDecrement()
                }, {
                    it.printStackTrace()
                }, {})
        }
    }

    companion object {
        @Container
        @JvmStatic
        val influxDBContainer = InfluxDBContainer<Nothing>(DockerImageName.parse("influxdb:2.1"))
            .apply {
                waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
                withEnv("DOCKER_INFLUXDB_INIT_MODE", "setup")
                withEnv("DOCKER_INFLUXDB_INIT_USERNAME", "user")
                withEnv("DOCKER_INFLUXDB_INIT_PASSWORD", "passpasspass")
                withEnv("DOCKER_INFLUXDB_INIT_ORG", "testtesttest")
                withEnv("DOCKER_INFLUXDB_INIT_BUCKET", "test")
                withEnv("DOCKER_INFLUXDB_INIT_RETENTION", "2d")
            }
    }
}
