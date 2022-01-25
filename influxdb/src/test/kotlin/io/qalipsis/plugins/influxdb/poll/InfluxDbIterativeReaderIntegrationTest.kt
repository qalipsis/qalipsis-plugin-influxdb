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
import java.time.Duration
import java.time.Instant
import java.util.concurrent.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test


@WithMockk
internal class InfluxDbIterativeReaderIntegrationTest : AbstractInfluxDbIntegrationTest() {

    private lateinit var reader: InfluxDbIterativeReader

    @RelaxedMockK
    private lateinit var stepStartStopContext: StepStartStopContext

    @Test
    //@Timeout(20)
    fun `should save data and poll client`() = runBlocking {

        val queryString = "from(bucket: my-bucket)"
        val pollStatement = InfluxDbPollStatement("time")
        reader = InfluxDbIterativeReader(
            clientFactory = { client },
            query = queryString,
            bindParameters = mutableMapOf("idle" to 90L),
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            resultsChannelFactory = { Channel(2) },
            coroutineScope = this,
            eventsLogger = null,
            meterRegistry = null,
            connectionConfiguration = connectionConfig
        )
        reader.init()

        reader.start(stepStartStopContext)
        reader.coInvokeInvisible<Unit>("poll", client)
        val point1: Point = Point.measurement("temperature")
            .addTag("location", "west")
            .addField("idle", 55L)
            .time(Instant.now().toEpochMilli(), WritePrecision.MS)
        val writeApi = client.writeApiBlocking
        writeApi.writePoint(point1)

        reader.coInvokeInvisible<Unit>("poll", client)
        val point2: Point = Point.measurement("temperature")
            .addTag("location", "west")
            .addField("idle", 65L)
            .time(Instant.now().toEpochMilli(), WritePrecision.MS)
        writeApi.writePoint(point2)
        reader.coInvokeInvisible<Unit>("poll", client)

        reader.coInvokeInvisible<Unit>("poll", client)

        val received1 = reader.next()
        val received2 = reader.next()
        val received3 = reader.next()
        assertThat(received1).all {
            /* hasSize(39)
             index(0).all {
                 key("device").isEqualTo("Car #1")
                 key("event").isEqualTo("Driving")
                 key("time").isEqualTo(BsonTimestamp(1603197368000))
             }
             index(12).all {
                 key("device").isEqualTo("Car #1")
                 key("event").isEqualTo("Stop")
                 key("time").isEqualTo(BsonTimestamp(1603198728000))
             }
             index(26).all {
                 key("device").isEqualTo("Truck #1")
                 key("event").isEqualTo("Driving")
                 key("time").isEqualTo(BsonTimestamp(1603197368000))
             }
             index(38).all {
                 key("device").isEqualTo("Truck #1")
                 key("event").isEqualTo("Stop")
                 key("time").isEqualTo(BsonTimestamp(1603198728000))
             }*/
        }

        assertThat(received2).all {

        }
        assertThat(received3).all {

        }

        reader.stop(relaxedMockk())
    }
}
