package io.qalipsis.plugins.influxdb.poll

import io.qalipsis.api.annotations.Scenario
import io.qalipsis.api.lang.concurrentList
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.rampup.regular
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.innerJoin
import io.qalipsis.api.steps.logErrors
import io.qalipsis.api.steps.map
import io.qalipsis.plugins.influxdb.influxdb
import java.time.Duration
import java.time.Instant
import org.influxdb.dto.Query

/**
 *
 * Scenario to demo how the poll step can work. The scenario reads the entries in a building on one side and the exits
 * on the other side.
 *
 * Records related to the same person are joined and the duration is then printed out in the console.
 *
 * @author Eric Jessé
 */
object PollScenario {

    private const val minions = 5

    val receivedMessages = concurrentList<String>()

    var mongoDbPort: Int = 0

    @JvmStatic
    private val log = logger()

    @Scenario
    fun pollData() {
        scenario("influxdb-poll") {
            minionsCount = minions
            rampUp {
                // Starts all at once.
                regular(100, minionsCount)
            }
        }
            .start()
            .influxdb().poll {
                name = "poll.in"
                connect { InfluxDbPollStepConnectionImpl() }
                query { "SELECT * FROM cpu WHERE idle  = \$idle AND system = \$system" }
                bindParameters("idle" to 90)
                bindParameters("system" to 5)
                pollDelay(Duration.ofSeconds(1))
            }
            .map { it.results }
            .logErrors()
            .innerJoin(
                using = { it.value["username"] },
                on = {
                    it.influxdb().poll {
                        name = "poll.out"
                        connect { InfluxDbPollStepConnectionImpl() }
                        query { "SELECT * FROM cpu WHERE idle  = \$idle AND system = \$system" }
                        bindParameters("idle" to 90)
                        bindParameters("system" to 5)
                        pollDelay(Duration.ofSeconds(1))
                    }.flatten()
                        .logErrors()
                        .map {
                            log.trace { "Right record: $it" }
                            it.results
                        }
                },
                having = { it.value["username"].also { log.trace { "Right: $it" } } }
            )
            .filterNotNull()
            .map { (inAction, outAction) ->
                val epochSecondIn = (inAction["timestamp"] as Instant).nano
                val epochSecondOut = (outAction["timestamp"] as Instant).nano

                inAction["username"] to Duration.ofSeconds(epochSecondOut - epochSecondIn)
            }
            .map { "The user ${it.first} stayed ${it.second.toMinutes()} minute(s) in the building" }
            .onEach { receivedMessages.add(it) }
            .onEach { println(it) }
    }

}
