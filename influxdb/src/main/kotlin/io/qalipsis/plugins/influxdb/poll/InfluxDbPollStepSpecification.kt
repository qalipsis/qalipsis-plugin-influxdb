package io.qalipsis.plugins.influxdb.poll

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.BroadcastSpecification
import io.qalipsis.api.steps.LoopableSpecification
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.plugins.influxdb.InfluxdbScenarioSpecification
import io.qalipsis.plugins.influxdb.InfluxdbStepSpecification
import java.time.Duration
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotNull
interface InfluxDbPollStepSpecification :
    StepSpecification<Unit, InfluxDbPollResults, InfluxDbPollStepSpecification>,
    InfluxdbStepSpecification<Unit, InfluxDbPollResults, InfluxDbPollStepSpecification>,
    LoopableSpecification, UnicastSpecification, BroadcastSpecification {
    /**
     * Configures the connection to the InfluxDB Server.
     */
    fun connect(connection: InfluxDbPollStepConnection.() -> Unit)

    /**
     * Creates the factory to execute to poll data.
     */
    fun query(queryFactory: String)

    /**
     * Parameters to bind to the query.
     *
     * @param param pairs of parameters names and values
     */
    fun bindParameters(vararg param: Pair<String, Any>)

    /**
     * Delay between two executions of poll.
     *
     * @param delay the delay to wait between the end of a poll and start of next one
     */
    fun pollDelay(delay: Duration)

    /**
     * Configures the monitoring of the poll step.
     */
    fun monitoring(monitoring: StepMonitoringConfiguration.() -> Unit)

}

/**
 * Interface to establish a connection with InfluxDb
 */
interface InfluxDbPollStepConnection {

    fun server(url: String, bucket: String, org: String)

    fun basic(user: String, password: String)

    fun enableGzip()

}

internal class InfluxDbPollStepSpecificationImpl(
) :  AbstractStepSpecification<Unit, InfluxDbPollResults, InfluxDbPollStepSpecification>(),
    InfluxDbPollStepSpecification {

    internal var searchConfig = InfluxDbSearchConfiguration()

    override val singletonConfiguration: SingletonConfiguration = SingletonConfiguration(SingletonType.UNICAST)

    val connectionConfiguration = InfluxDbPollStepConnectionImpl()

    val monitoringConfiguration = StepMonitoringConfiguration()

    @field:NotNull
    internal lateinit var query: String

    @field:NotNull
    internal var pollPeriod: Duration = Duration.ofSeconds(10L)

    internal val bindParameters: MutableMap<@NotBlank String, Any> = mutableMapOf()

    override fun connect(connection: InfluxDbPollStepConnection.() -> Unit) {
        this.connectionConfiguration.connection()
    }
    override fun query(queryFactory: String) {
        query = queryFactory
    }

    override fun bindParameters(vararg param: Pair<String, Any>) {
        this.bindParameters.clear()
        this.bindParameters.putAll(param)
    }

    override fun pollDelay(delay: Duration) {
        pollPeriod = delay
    }

    override fun monitoring(monitoring: StepMonitoringConfiguration.() -> Unit) {
        monitoringConfiguration.monitoring()
    }
}

internal class InfluxDbPollStepConnectionImpl : InfluxDbPollStepConnection {

    @field:NotBlank
    var url = "http://127.0.0.1:8086"

    @field:NotBlank
    var bucket = ""

    var user: String = ""

    var password: String = ""

    var org: String = ""

    var gzipEnabled = false

    override fun server(url: String, bucket: String, org: String) {
        this.url = url
        this.bucket = bucket
        this.org = org
    }

    override fun basic(user: String, password: String) {
        this.user = user
        this.password = password
    }

    override fun enableGzip() {
        this.gzipEnabled = true
    }
}

/**
 * @property database name of db to search
 * @property collection collection in db (table in sql)
 * @property query [QueryResult] query for search
 * @property tieBreaker defines the name, which is the value used to limit the records for the next poll.
 * The tie-breaker must be used as the first sort clause of the query and always be not null. Only the records
 * from the database having a [tieBreaker] greater (or less if sorted descending) than the last polled value will be fetched at next poll.
 */
@Spec
data class InfluxDbSearchConfiguration(
    @field:NotBlank var tieBreaker: String = "time"
)

    /**
 * Creates an InfluxDB poll step in order to periodically fetch data from an InfluxDB server.
 *
 * This step is generally used in conjunction with a left join to assert data or inject them in a workflow
 *
 * @author Eric Jessé
 */
fun InfluxdbScenarioSpecification.poll(
    configurationBlock: InfluxDbPollStepSpecification.() -> Unit
): InfluxDbPollStepSpecification {
    val step = InfluxDbPollStepSpecificationImpl()
    step.configurationBlock()

    (this as StepSpecificationRegistry).add(step)
    return step
}
