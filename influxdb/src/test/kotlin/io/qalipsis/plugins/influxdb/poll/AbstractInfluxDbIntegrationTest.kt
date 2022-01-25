package io.qalipsis.plugins.influxdb.poll

import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.test.mockk.WithMockk
import java.time.Duration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.InfluxDBContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.math.pow


@WithMockk
@Testcontainers
internal abstract class AbstractInfluxDbIntegrationTest {

    lateinit var client: InfluxDBClient

    protected val connectionConfig = InfluxDbPollStepConnectionImpl()

    @BeforeAll
    fun beforeAll() {
        connectionConfig.url = influxDBContainer.url
        connectionConfig.password = "passpasspass"
        connectionConfig.user = "user"
        connectionConfig.org = "testtesttest"
        connectionConfig.bucket = "test"
        client = InfluxDBClientFactory.create(connectionConfig.url, "user", "passpasspass".toCharArray())
    }

    @AfterAll
    fun afterAll() {
        client.close()
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
                withEnv("DOCKER_INFLUXDB_INIT_RETENTION", "1d")
            }

        @JvmStatic
        val log = logger()
    }

}
