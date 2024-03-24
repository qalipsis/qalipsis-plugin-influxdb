/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.influxdb.config

import assertk.all
import assertk.assertThat
import assertk.assertions.any
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.prop
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.plugins.influxdb.meters.InfluxDbMeasurementConfiguration
import io.qalipsis.plugins.influxdb.meters.InfluxdbMeasurementPublisher
import io.qalipsis.test.assertk.typedProp
import jakarta.inject.Inject
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

internal class InfluxDbMeterRegistryConfigIntegrationTest {

    @Nested
    @MicronautTest(environments = ["influxdb"], startApplication = false)
    inner class WithoutRegistry {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        fun `should start without the registry`() {
            assertThat(applicationContext.getBeansOfType(InfluxdbMeasurementPublisher::class.java)).isEmpty()
            assertThat(applicationContext.getBeansOfType(MeasurementPublisherFactory::class.java)).isEmpty()
        }
    }

    @Nested
    @MicronautTest(environments = ["influxdb"], startApplication = false)
    inner class WithConfiguredRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        internal fun `should start with the configured registry`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisher::class.java)).any {
                it.isInstanceOf(InfluxdbMeasurementPublisher::class)
            }

            assertThat(applicationContext.getBean(InfluxdbMeasurementPublisher::class.java))
                .typedProp<InfluxDbMeasurementConfiguration>(
                    "configuration"
                )
                .all {
                    prop(InfluxDbMeasurementConfiguration::url).isEqualTo("http://localhost:8086")
                    prop(InfluxDbMeasurementConfiguration::bucket).isEqualTo("qalipsis-meter")
                    prop(InfluxDbMeasurementConfiguration::org).isEqualTo("qalipsis")
                    prop(InfluxDbMeasurementConfiguration::userName).isEqualTo("")
                    prop(InfluxDbMeasurementConfiguration::password).isEqualTo("")
                }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.influxdb.enabled" to StringUtils.TRUE,
                "meters.export.influxdb.db" to "qalipsis-meters-db",
                "meters.export.influxdb.uri" to "http://localhost:8086",
            )
        }
    }

    @Nested
    @MicronautTest(environments = ["influxdb"], startApplication = false)
    inner class WithRegistry : TestPropertyProvider {

        @Inject
        private lateinit var applicationContext: ApplicationContext

        @Test
        @Timeout(4)
        internal fun `should start with the registry`() {
            assertThat(applicationContext.getBeansOfType(MeasurementPublisher::class.java)).any {
                it.isInstanceOf(InfluxdbMeasurementPublisher::class)
            }

            assertThat(applicationContext.getBean(InfluxdbMeasurementPublisher::class.java)).typedProp<InfluxDbMeasurementConfiguration>(
                "configuration"
            )
                .all {
                    prop(InfluxDbMeasurementConfiguration::url).isEqualTo("http://localhost:8086")
                    prop(InfluxDbMeasurementConfiguration::bucket).isEqualTo("qalipsis-meter")
                    prop(InfluxDbMeasurementConfiguration::org).isEqualTo("qalipsis")
                    prop(InfluxDbMeasurementConfiguration::userName).isEqualTo("")
                    prop(InfluxDbMeasurementConfiguration::password).isEqualTo("")
                }

            val measurementPublisherFactory = applicationContext.getBean(MeasurementPublisherFactory::class.java)
            var generatedMeterPublisherRegistry = measurementPublisherFactory.getPublisher()
            assertThat(generatedMeterPublisherRegistry).typedProp<InfluxDbMeasurementConfiguration>("configuration")
                .all {
                    prop(InfluxDbMeasurementConfiguration::url).isEqualTo("http://localhost:8086")
                    prop(InfluxDbMeasurementConfiguration::bucket).isEqualTo("qalipsis-meter")
                    prop(InfluxDbMeasurementConfiguration::org).isEqualTo("qalipsis")
                    prop(InfluxDbMeasurementConfiguration::userName).isEqualTo("")
                    prop(InfluxDbMeasurementConfiguration::password).isEqualTo("")
                }

            generatedMeterPublisherRegistry = measurementPublisherFactory.getPublisher()
            assertThat(generatedMeterPublisherRegistry).typedProp<InfluxDbMeasurementConfiguration>("configuration")
                .all {
                    prop(InfluxDbMeasurementConfiguration::url).isEqualTo("http://localhost:8086")
                    prop(InfluxDbMeasurementConfiguration::bucket).isEqualTo("qalipsis-meter")
                    prop(InfluxDbMeasurementConfiguration::org).isEqualTo("qalipsis")
                    prop(InfluxDbMeasurementConfiguration::userName).isEqualTo("")
                    prop(InfluxDbMeasurementConfiguration::password).isEqualTo("")
                }
        }

        override fun getProperties(): Map<String, String> {
            return mapOf(
                "meters.export.influxdb.enabled" to StringUtils.TRUE,
                "meters.export.influxdb.db" to "qalipsis-meters-db",
            )
        }
    }
}