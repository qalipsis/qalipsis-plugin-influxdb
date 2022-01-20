package io.qalipsis.plugins.influxdb.poll

import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.concurrent.CountDownLatch
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import org.testcontainers.containers.InfluxDBContainer
import kotlin.math.pow

@WithMockk
@Testcontainers
internal abstract class AbstractInfluxDbIntegrationTest {

    lateinit var client: InfluxDB

    protected val connectionConfig = InfluxDbPollStepConnectionImpl()

    @BeforeAll
    fun beforeAll() {
        connectionConfig.password = "password"
        connectionConfig.url = influxDBContainer.url
        connectionConfig.username = "admin"
        connectionConfig.database = "myDB"

        client = InfluxDBFactory.connect(connectionConfig.url,
            connectionConfig.username, connectionConfig.password)
    }

    @AfterAll
    fun afterAll() {
        client.close()
    }

    companion object {

        @Container
        @JvmStatic
        val influxDBContainer = InfluxDBContainer<Nothing>(DockerImageName.parse("influxdb:1.8"))
            .apply {
                waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
            }

        @JvmStatic
        val log = logger()
    }

}
