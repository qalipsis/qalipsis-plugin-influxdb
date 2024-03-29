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

import io.micrometer.core.instrument.Clock
import io.micrometer.influx.InfluxConfig
import io.micrometer.influx.InfluxMeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.naming.conventions.StringConvention
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.api.meters.MeterRegistryConfiguration
import io.qalipsis.api.meters.MeterRegistryFactory
import jakarta.inject.Singleton
import java.time.Duration
import java.util.Properties

/**
 * Configuration for the export of micrometer [io.micrometer.core.instrument.Meter] to InfluxDb.
 *
 * @author Palina Bril
 */
@Factory
@Requirements(
    Requires(property = MetersConfig.EXPORT_ENABLED, notEquals = StringUtils.FALSE),
    Requires(property = InfluxDbMeterRegistryFactory.INBLUXDB_ENABLED, notEquals = StringUtils.FALSE)
)
internal class InfluxDbMeterRegistryFactory(environment: Environment) : MeterRegistryFactory {

    private val properties = Properties()

    init {
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.RAW))
        properties.putAll(environment.getProperties(MetersConfig.EXPORT_CONFIGURATION, StringConvention.CAMEL_CASE))
    }

    @Singleton
    fun influxdbRegistry(): InfluxMeterRegistry {
        return InfluxMeterRegistry(
            object : InfluxConfig {
                override fun prefix() = "influxdb"
                override fun org(): String = "qalipsis"
                override fun bucket(): String = "qalipsis-event"
                override fun userName(): String = ""
                override fun password(): String = ""
                override fun token(): String = "any"
                override fun get(key: String): String? {
                    return properties.getProperty(key)
                }
            },
            Clock.SYSTEM
        )
    }

    override fun getRegistry(configuration: MeterRegistryConfiguration): InfluxMeterRegistry {
        return InfluxMeterRegistry(
            object : InfluxConfig {
                override fun prefix() = "influxdb"
                override fun step(): Duration = configuration.step ?: super.step()
                override fun org(): String = "qalipsis"
                override fun bucket(): String = "qalipsis-event"
                override fun userName(): String = ""
                override fun password(): String = ""
                override fun token(): String = "any"
                override fun get(key: String): String? {
                    return properties.getProperty(key)
                }
            },
            Clock.SYSTEM
        )
    }

    companion object {

        private const val INFLUXDB_CONFIGURATION = "${MetersConfig.EXPORT_CONFIGURATION}.influxdb"

        const val INBLUXDB_ENABLED = "$INFLUXDB_CONFIGURATION.enabled"
    }
}
