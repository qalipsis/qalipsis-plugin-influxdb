package io.qalipsis.plugins.influxdb.poll

import assertk.all
import assertk.assertThat
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.mockk.impl.annotations.RelaxedMockK
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import com.influxdb.query.FluxRecord
import io.qalipsis.test.assertk.prop
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.Period

@WithMockk
internal class InfluxDbIterativeReaderIntegrationTest : AbstractInfluxDbIntegrationTest() {

    private lateinit var reader: InfluxDbIterativeReader

    @RelaxedMockK
    private lateinit var stepStartStopContext: StepStartStopContext

    @Test
    //@Timeout(20)
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
          assertThat(received1.queryResults).all {
              hasSize(1)
              index(0).isInstanceOf(FluxRecord::class).all {
                  prop("values").isNotNull()
              }
        }

        assertThat(received2.queryResults).all {
            hasSize(2)
            index(0).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
            index(1).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
        }
        assertThat(received3.queryResults).all {
            hasSize(1)
            index(0).isInstanceOf(FluxRecord::class).all {
                prop("values").isNotNull()
            }
        }
        reader.stop(relaxedMockk())
    }
}
