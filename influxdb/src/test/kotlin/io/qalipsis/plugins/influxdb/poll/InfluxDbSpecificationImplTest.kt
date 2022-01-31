package io.qalipsis.plugins.influxdb.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isTrue
import assertk.assertions.prop
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.influxdb.influxdb
import java.time.Duration
import org.junit.jupiter.api.Test

internal class InfluxDbSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the scenario with default values`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.influxdb().poll {
            name = "my-step"
        }
        assertThat(scenario.rootSteps.first()).isInstanceOf(InfluxDbPollStepSpecificationImpl::class).all {
            prop(InfluxDbPollStepSpecificationImpl::name).isEqualTo("my-step")
            prop(InfluxDbPollStepSpecificationImpl::pollPeriod).isEqualTo(
                Duration.ofSeconds(10L)
            )
            prop(InfluxDbPollStepSpecificationImpl::monitoringConfiguration).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
            prop(InfluxDbPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(-1)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ZERO)
            }
        }
    }

    @Test
    internal fun `should add a complete specification to the scenario as broadcast whit monitoring`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.influxdb().poll {
            name = "my-step"
            connect {
                server("http://127.0.0.1:8086","DB", "org")
                basic("user","pass")
            }
            pollDelay(Duration.ofSeconds(1L))
            monitoring {
                events = false
                meters = true
            }
            broadcast(123, Duration.ofSeconds(20))
        }
        assertThat(scenario.rootSteps.first()).isInstanceOf(InfluxDbPollStepSpecificationImpl::class).all {
            prop(InfluxDbPollStepSpecificationImpl::name).isEqualTo("my-step")

            prop(InfluxDbPollStepSpecificationImpl::pollPeriod).isEqualTo(Duration.ofSeconds(1L))
            prop(InfluxDbPollStepSpecificationImpl::monitoringConfiguration).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
            prop(InfluxDbPollStepSpecificationImpl::connectionConfiguration).isInstanceOf(InfluxDbPollStepConnectionImpl::class).all {
                prop(InfluxDbPollStepConnectionImpl::bucket).isEqualTo("DB")
                prop(InfluxDbPollStepConnectionImpl::user).isEqualTo("user")
                prop(InfluxDbPollStepConnectionImpl::url).isEqualTo("http://127.0.0.1:8086")
                prop(InfluxDbPollStepConnectionImpl::password).isEqualTo("pass")
            }
            prop(InfluxDbPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.BROADCAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(123)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofSeconds(20))
            }
        }
    }

    @Test
    internal fun `should add a complete specification to the scenario as broadcast whit logger`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.influxdb().poll {
            name = "my-step"
            connect {
                server("http://127.0.0.1:8086","DB","org")
                basic("user","pass")
            }
            pollDelay(Duration.ofSeconds(1L))
            monitoring {
                events = true
                meters = false
            }
            broadcast(123, Duration.ofSeconds(20))
        }
        assertThat(scenario.rootSteps.first()).isInstanceOf(InfluxDbPollStepSpecificationImpl::class).all {
            prop(InfluxDbPollStepSpecificationImpl::name).isEqualTo("my-step")

            prop(InfluxDbPollStepSpecificationImpl::pollPeriod).isEqualTo(Duration.ofSeconds(1L))
            prop(InfluxDbPollStepSpecificationImpl::monitoringConfiguration).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
            prop(InfluxDbPollStepSpecificationImpl::connectionConfiguration).isInstanceOf(InfluxDbPollStepConnectionImpl::class).all {
                prop(InfluxDbPollStepConnectionImpl::bucket).isEqualTo("DB")
                prop(InfluxDbPollStepConnectionImpl::user).isEqualTo("user")
                prop(InfluxDbPollStepConnectionImpl::url).isEqualTo("http://127.0.0.1:8086")
                prop(InfluxDbPollStepConnectionImpl::password).isEqualTo("pass")
            }
            prop(InfluxDbPollStepSpecificationImpl::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.BROADCAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(123)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofSeconds(20))
            }
        }
    }

}
