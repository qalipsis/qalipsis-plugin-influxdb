package io.qalipsis.plugins.influxdb.poll


import assertk.all
import assertk.assertThat
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.impl.annotations.SpyK
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import java.time.Duration
import java.util.concurrent.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.influxdb.InfluxDB
import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.BatchPoints
import org.influxdb.dto.Point
import org.influxdb.dto.QueryResult
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout


@WithMockk
internal class InfluxDbIterativeReaderIntegrationTest : AbstractInfluxDbIntegrationTest() {

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private lateinit var reader: InfluxDbIterativeReader

    private val clientBuilder: () -> InfluxDB = { client }

    @RelaxedMockK
    private lateinit var resultsChannel: Channel<InfluxDbQueryResult>

    @SpyK
    private var resultsChannelFactory: () -> Channel<InfluxDbQueryResult> = { resultsChannel }

    @AfterEach
    @Timeout(5)
    fun afterEach() {
        reader.stop(relaxedMockk())
    }

    @Test
    @Timeout(20)
    fun `should save data and poll client`() = runBlocking {
        val clientBuilder: (() -> InfluxDB) =
            {
                InfluxDBFactory.connect(connectionConfig.url,
                    connectionConfig.username, connectionConfig.password)
            }
        val batchPoints = BatchPoints
            .database(connectionConfig.database)
            .tag("async", "true")
            .retentionPolicy("aRetentionPolicy")
            .consistency(ConsistencyLevel.ALL)
            .build()
        val point1: Point = Point.measurement("cpu")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("idle", 90L)
            .addField("user", 9L)
            .addField("system", 1L)
            .build()
        val point2: Point = Point.measurement("cpu")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("idle", 90L)
            .addField("user", 10L)
            .addField("system", 12L)
            .build()

        batchPoints.point(point1);
        batchPoints.point(point2);
        clientBuilder().write(batchPoints);

        val pollStatement = InfluxDbPollStatement()
        reader = InfluxDbIterativeReader(
            clientBuilder = clientBuilder,
            query = { "SELECT * FROM cpu WHERE idle  = \$idle" },
            bindParameters = mutableMapOf("idle" to 90),
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            resultsChannelFactory = resultsChannelFactory,
            coroutineScope = this,
            eventsLogger = null,
            meterRegistry = null,
            connectionConfiguration = connectionConfig
        )

        reader.start(relaxedMockk())

        Assertions.assertTrue(reader.hasNext())

        val received: QueryResult = reader.next().queryResult

        assertThat(received).all {
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

        reader.stop(relaxedMockk())
    }
}
