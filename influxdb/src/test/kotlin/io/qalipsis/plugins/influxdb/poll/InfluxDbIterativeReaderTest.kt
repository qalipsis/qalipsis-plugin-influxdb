package io.qalipsis.plugins.influxdb.poll

import assertk.assertThat
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNotSameAs
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import io.aerisconsulting.catadioptre.getProperty
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.impl.annotations.SpyK
import io.mockk.spyk
import io.mockk.verify
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runBlockingTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration

@WithMockk
internal class InfluxDbIterativeReaderTest {

    @RelaxedMockK
    private lateinit var resultsChannel: Channel<InfluxDbQueryResult>

    @SpyK
    private var resultsChannelFactory: () -> Channel<InfluxDbQueryResult> = { resultsChannel }

    @RelaxedMockK
    private lateinit var pollStatement: InfluxDbPollStatement

    @RelaxedMockK
    private lateinit var eventsLogger: EventsLogger

    @RelaxedMockK
    private lateinit var meterRegistry: MeterRegistry

   /* @Test
    //@Timeout(25)
    internal fun `should be restartable`() = runBlocking {
        val connectionConfig  = InfluxDbPollStepConnectionImpl()

        connectionConfig.password = "pass"
        connectionConfig.url = "http://127.0.0.1:8086"
        connectionConfig.username = "name"
        connectionConfig.bucket = "db"
        // given
        val latch = SuspendedCountLatch(1, true)
        val reader = spyk(
            InfluxDbIterativeReader(
                clientFactory = { InfluxDBFactory.connect(connectionConfig.url, connectionConfig.username, connectionConfig.password) },
                query =  "SELECT * FROM cpu WHERE idle  = \$idle" ,
                bindParameters = mutableMapOf("idle" to 90),
                pollStatement = pollStatement,
                pollDelay = Duration.ofMillis(300),
                resultsChannelFactory = resultsChannelFactory,
                coroutineScope = this,
                eventsLogger = eventsLogger,
                meterRegistry = meterRegistry,
                connectionConfiguration = connectionConfig
            ), recordPrivateCalls = true
        )
        coEvery { reader["poll"](any<InfluxDB>()) } coAnswers { latch.decrement() }


        // when
        reader.start(relaxedMockk { })

        // then
        latch.await()
        verify { reader["init"]() }
        verify { resultsChannelFactory() }
        assertThat(reader.hasNext()).isTrue()
        val pollingJob = reader.getProperty<Job>("pollingJob")
        assertThat(pollingJob).isNotNull()
        val client = reader.getProperty<InfluxDB>("client")
        assertThat(client).isNotNull()
        val resultsChannel = reader.getProperty<Channel<InfluxDbQueryResult>>("resultsChannel")
        assertThat(resultsChannel).isSameAs(resultsChannel)

        // when
        reader.stop(relaxedMockk())
        verify { resultsChannel.cancel() }
        verify { pollStatement.reset() }
        // then
        assertThat(reader.hasNext()).isFalse()

        // when
        latch.reset()
        reader.start(relaxedMockk { })
        // then
        verify { reader["init"]() }
        verify { resultsChannelFactory() }
        assertThat(reader.hasNext()).isTrue()
        assertThat(reader.getProperty<Job>("pollingJob")).isNotSameAs(pollingJob)
        assertThat(reader.getProperty<InfluxDB>("client")).isInstanceOf(InfluxDB::class)
        assertThat(reader.getProperty<Channel<InfluxDbQueryResult>>("resultsChannel")).isSameAs(resultsChannel)

        reader.stop(relaxedMockk())
    }

    @Test
    @Timeout(10)
    fun `should be empty before start`() = runBlockingTest {
        val connectionConfig  = InfluxDbPollStepConnectionImpl()

        connectionConfig.password = "pass"
        connectionConfig.url = "http://127.0.0.1:8086"
        connectionConfig.username = "name"
        connectionConfig.bucket = "db"
        // given
        val reader =  spyk(
                InfluxDbIterativeReader(
                    clientFactory = { InfluxDBFactory.connect(connectionConfig.url, connectionConfig.username, connectionConfig.password) },
                    query =  "SELECT * FROM cpu WHERE idle  = \$idle" ,
                    bindParameters = mutableMapOf("idle" to 90),
                    pollStatement = pollStatement,
                    pollDelay = Duration.ofMillis(300),
                    resultsChannelFactory = resultsChannelFactory,
                    coroutineScope = this,
                    eventsLogger = eventsLogger,
                    meterRegistry = meterRegistry,
                    connectionConfiguration = connectionConfig
            ), recordPrivateCalls = true
        )

        // then
        assertThat(reader.hasNext()).isFalse()
        assertThat(reader.getProperty<Channel<InfluxDbQueryResult>>("resultsChannel")).isNull()
    }

    @Test
    @Timeout(20)
    fun `should poll at least twice after start`() = runBlocking {
        // given
        val connectionConfig  = InfluxDbPollStepConnectionImpl()

        connectionConfig.password = "pass"
        connectionConfig.url = "http://127.0.0.1:8086"
        connectionConfig.username = "name"
        connectionConfig.bucket = "db"
        val reader = spyk(
            InfluxDbIterativeReader(
                clientFactory = { InfluxDBFactory.connect(connectionConfig.url, connectionConfig.username, connectionConfig.password) },
                query =  "SELECT * FROM cpu WHERE idle  = \$idle",
                bindParameters = mutableMapOf("idle" to 90),
                pollStatement = pollStatement,
                pollDelay = Duration.ofMillis(300),
                resultsChannelFactory = resultsChannelFactory,
                coroutineScope = this,
                eventsLogger = eventsLogger,
                meterRegistry = meterRegistry,
                connectionConfiguration = connectionConfig
            ), recordPrivateCalls = true
        )
        val countDownLatch = SuspendedCountLatch(2, true)
        coEvery { reader["poll"](any<InfluxDB>()) } coAnswers { countDownLatch.decrement() }

        // when
        reader.start(relaxedMockk())
        countDownLatch.await()

        // then
        coVerify(atLeast = 2) { reader["poll"](any<InfluxDB>()) }
        assertThat(reader.hasNext()).isTrue()

        reader.stop(relaxedMockk())
    }*/
}
