package io.qalipsis.plugins.influxdb.poll

import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.steps.BroadcastSpecification
import io.qalipsis.api.steps.LoopableSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.plugins.influxdb.InfluxdbScenarioSpecification
import io.qalipsis.plugins.influxdb.InfluxdbStepSpecification
import org.influxdb.dto.Query
import java.time.Duration
import javax.validation.constraints.NotBlank

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
    fun query(queryFactory: () -> Query)

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

interface InfluxDbPollStepConnection {

    fun server(url: String, database: String)

    fun basic(username: String, password: String)

    fun enableGzip()

}

internal class InfluxDbPollStepSpecificationImpl : InfluxDbPollStepSpecification {

    val connectionConfiguration = InfluxDbPollStepConnectionImpl()

    val monitoringConfiguration = StepMonitoringConfiguration()

    override fun connect(connection: InfluxDbPollStepConnection.() -> Unit) {
        this.connectionConfiguration.connection()
    }

    override fun query(queryFactory: () -> Query) {
        TODO("Not yet implemented")
    }

    override fun bindParameters(vararg param: Pair<String, Any>) {
        TODO("Not yet implemented")
    }

    override fun pollDelay(delay: Duration) {
        TODO("Not yet implemented")
    }

    override fun monitoring(monitoring: StepMonitoringConfiguration.() -> Unit) {
        monitoringConfiguration.monitoring()
    }
}

internal class InfluxDbPollStepConnectionImpl : InfluxDbPollStepConnection {

    @field:NotBlank
    var url = "http://127.0.0.1:8086"

    @field:NotBlank
    var database = ""

    var username: String? = null

    var password: String? = null

    var gzipEnabled = false

    override fun server(url: String, database: String) {
        this.url = url
        this.database = database
    }

    override fun basic(username: String, password: String) {
        this.username = username
        this.password = password
    }

    override fun enableGzip() {
        this.gzipEnabled = true
    }
}


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
