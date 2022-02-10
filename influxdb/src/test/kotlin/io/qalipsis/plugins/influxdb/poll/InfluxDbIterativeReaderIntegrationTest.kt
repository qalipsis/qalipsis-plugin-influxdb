package io.qalipsis.plugins.influxdb.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import com.influxdb.client.domain.WritePrecision
import com.influxdb.query.FluxRecord
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.time.Instant
import java.time.Period


internal class InfluxDbIterativeReaderIntegrationTest : AbstractInfluxDbIntegrationTest() {

    private lateinit var reader: InfluxDbIterativeReader

    @Test
    @Timeout(20)
    fun `should save data and poll client`() = runBlocking {

        client.bucketsApi.findBuckets()
        val queryString = "from(bucket: \"test\")"
        val pollStatement = InfluxDbPollStatement()
        reader = InfluxDbIterativeReader(
            clientFactory = { client },
            query = queryString,
            bindParameters = mutableMapOf(),
            sortFields = mutableListOf(),
            desc = false,
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            resultsChannelFactory = { Channel(2) }, // The capacity is perhaps to small, preventing from data to be written by the poll action.
            coroutineScope = this,
            eventsLogger = null,
            meterRegistry = null,
            connectionConfiguration = connectionConfig
        )
        reader.init()

        val writeApi = client.writeApiBlocking

        val data1 = "moves,host1=host1 idle=\"55\" " + Instant.now().minus(Period.ofDays(2)).toEpochMilli() * 1000000
        writeApi.writeRecord("test", "testtesttest", WritePrecision.NS, data1)

        reader.coInvokeInvisible<Unit>("poll", client)

        val data2 = "moves,host2=host2 idle=\"80\" " + Instant.now().minus(Period.ofDays(1)).toEpochMilli() * 1000000
        writeApi.writeRecord("test", "testtesttest", WritePrecision.NS, data2)

        reader.coInvokeInvisible<Unit>("poll", client)
        reader.coInvokeInvisible<Unit>("poll", client)

        val received1 = reader.next()
        val received2 = reader.next()
        val received3 = reader.next()

        Assertions.assertEquals("55", received1.results[0].value)
        Assertions.assertEquals("host1", received1.results[0].values["host1"])
        assertThat(received1.results).all {
            hasSize(1)
            index(0).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
        }

        Assertions.assertEquals("55", received2.results[0].value)
        Assertions.assertEquals("host1", received2.results[0].values["host1"])

        Assertions.assertEquals("80", received2.results[1].value)
        Assertions.assertEquals("host2", received2.results[1].values["host2"])
        assertThat(received2.results).all {
            hasSize(2)

            index(0).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
            index(1).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
        }

        Assertions.assertEquals("80", received3.results[0].value)
        Assertions.assertEquals("host2", received3.results[0].values["host2"])
        assertThat(received3.results).all {
            hasSize(1)
            index(0).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
        }
        reader.stop(relaxedMockk())
    }
}
