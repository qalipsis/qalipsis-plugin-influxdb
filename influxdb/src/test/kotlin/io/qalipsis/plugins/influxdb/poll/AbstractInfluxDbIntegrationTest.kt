package io.qalipsis.plugins.influxdb.poll

import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.domain.Authorization
import com.influxdb.client.domain.BucketRetentionRules
import com.influxdb.client.domain.Permission
import com.influxdb.client.domain.PermissionResource
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
        connectionConfig.token = "".toCharArray()
        connectionConfig.url = influxDBContainer.url

        connectionConfig.org = "my-org"
        connectionConfig.bucket = "my-bucket"
        val influxDBClient = InfluxDBClientFactory.create(connectionConfig.url, connectionConfig.token,  connectionConfig.org)
        val retention = BucketRetentionRules()
        retention.everySeconds = 3600

        val bucket = influxDBClient.bucketsApi.createBucket("my-bucket", retention, "12bdc4164c2e8141")
        val resource = PermissionResource()
        resource.id = bucket.id
        resource.orgID = "12bdc4164c2e8141"
        resource.type = PermissionResource.TypeEnum.BUCKETS

        // Read permission
        val read = Permission()
        read.resource = resource
        read.action = Permission.ActionEnum.READ
        val write = Permission()
        write.resource = resource
        write.action = Permission.ActionEnum.WRITE

        val authorization: Authorization = influxDBClient.authorizationsApi
            .createAuthorization("12bdc4164c2e8141", listOf(read, write))
        val token = authorization.token

        /*influxDBClient.bucketsApi.createBucket(connectionConfig.bucket, "1")
        influxDBClient.organizationsApi.createOrganization(connectionConfig.org)*/
        influxDBClient.close()
        client = InfluxDBClientFactory.create(connectionConfig.url,
            token.toCharArray(), connectionConfig.org, connectionConfig.bucket)

        /*client.bucketsApi.createBucket(connectionConfig.bucket, "1")
        client.organizationsApi.createOrganization(connectionConfig.org)
        client = InfluxDBClientFactory.create(connectionConfig.url,
            connectionConfig.token, connectionConfig.org, connectionConfig.bucket)*/
        /*client.query( Query("CREATE DATABASE " + connectionConfig.bucket))
        client.setDatabase(connectionConfig.bucket)*/
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
            }

        @JvmStatic
        val log = logger()
    }

}
