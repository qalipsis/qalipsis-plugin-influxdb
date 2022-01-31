package io.qalipsis.plugins.influxdb.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import com.influxdb.query.FluxRecord
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.relaxedMockk
import java.time.Duration
import java.time.Instant
import java.time.Period
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

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
            bindParameters = mutableMapOf("_value" to "55"),
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            resultsChannelFactory = { Channel(2) }, // The capacity is perhaps to small, preventing from data to be written by the poll action.
            coroutineScope = this,
            eventsLogger = null,
            meterRegistry = null,
            connectionConfiguration = connectionConfig
        )
        reader.init()

        val point1: Point = Point.measurement("temperature")
            .addTag("location", "west1")
            .addField("idle", "55")
            .time(Instant.now().minus(Period.ofDays(1)), WritePrecision.MS)
        val writeApi = client.writeApiBlocking
        writeApi.writePoint(point1)

        reader.coInvokeInvisible<Unit>("poll", client)
        val point2: Point = Point.measurement("temperature")
            .addTag("location", "west2")
            .addField("idle", "55")
            .time(Instant.now().toEpochMilli(), WritePrecision.MS)
        writeApi.writePoint(point2)
        reader.coInvokeInvisible<Unit>("poll", client)
        reader.coInvokeInvisible<Unit>("poll", client)

        val received1 = reader.next()
        val received2 = reader.next()
        val received3 = reader.next()

        Assertions.assertEquals("55", received2.results[0].value)
        Assertions.assertEquals("west1", received2.results[0].values["location"])
        assertThat(received1.results).all {
              hasSize(1)
              index(0).isInstanceOf(FluxRecord::class).all {
                  prop("values").isNotNull()
              }
        }

        Assertions.assertEquals("55", received2.results[0].value)
        Assertions.assertEquals("west1", received2.results[0].values["location"])

        Assertions.assertEquals("55", received2.results[1].value)
        Assertions.assertEquals("west2", received2.results[1].values["location"])
        assertThat(received2.results).all {
            hasSize(2)

            index(0).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
            index(1).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
        }

        Assertions.assertEquals("55", received3.results[0].value)
        Assertions.assertEquals("west2", received3.results[0].values["location"])
        assertThat(received3.results).all {
            hasSize(1)
            index(0).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
        }
        reader.stop(relaxedMockk())
    }
}
