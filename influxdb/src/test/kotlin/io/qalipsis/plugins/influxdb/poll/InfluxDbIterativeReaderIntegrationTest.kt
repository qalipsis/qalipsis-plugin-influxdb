package io.qalipsis.plugins.influxdb.poll


import assertk.all
import assertk.assertThat
import io.aerisconsulting.catadioptre.coInvokeInvisible
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.impl.annotations.SpyK
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import java.time.Duration
import java.util.concurrent.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.influxdb.BatchOptions
import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.BatchPoints
import org.influxdb.dto.Point
import org.influxdb.dto.Query
import org.influxdb.dto.QueryResult
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout


@WithMockk
internal class InfluxDbIterativeReaderIntegrationTest : AbstractInfluxDbIntegrationTest() {

    private val eventsLogger = relaxedMockk<EventsLogger>()

    private lateinit var reader: InfluxDbIterativeReader

    @RelaxedMockK
    private lateinit var resultsChannel: Channel<InfluxDbQueryResult>

    @SpyK
    private var resultsChannelFactory: () -> Channel<InfluxDbQueryResult> = { resultsChannel }

    @Test
    //@Timeout(20)
    fun `should save data and poll client`() = runBlocking {

        client.query( Query("CREATE DATABASE " + connectionConfig.database))
        client.setDatabase(connectionConfig.database);

        val retentionPolicyName = "one_day_only"
        client.query(
            Query(
                "CREATE RETENTION POLICY " + retentionPolicyName
                        + " ON " + connectionConfig.database + " DURATION 1d REPLICATION 1 DEFAULT"
            )
        )
        client.setRetentionPolicy(retentionPolicyName) // (3)


        client.enableBatch(
            BatchOptions.DEFAULTS
                .threadFactory { runnable: Runnable? ->
                    val thread = Thread(runnable)
                    thread.isDaemon = true
                    thread
                }
        )
        val batchPoints = BatchPoints
            .database(connectionConfig.database)
            .tag("async", "true")
            .retentionPolicy(retentionPolicyName)
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
        client.write(batchPoints);
        val queryString = "SELECT * FROM cpu WHERE idle  = \$idle"
        val pollStatement = InfluxDbPollStatement("time")
        reader = InfluxDbIterativeReader(
            clientFactory = { client },
            query = queryString,
            bindParameters = mutableMapOf("idle" to 90L),
            pollStatement = pollStatement,
            pollDelay = Duration.ofMillis(300),
            resultsChannelFactory = resultsChannelFactory,
            coroutineScope = this,
            eventsLogger = null,
            meterRegistry = null,
            connectionConfiguration = connectionConfig
        )

        reader.start(relaxedMockk())
        reader.coInvokeInvisible<Unit>("poll", client)
        reader.coInvokeInvisible<Unit>("poll", client)

        Assertions.assertTrue(reader.hasNext())

        val received = reader.next().queryResult

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
