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

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requirements
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.core.util.StringUtils
import io.qalipsis.api.config.MetersConfig
import io.qalipsis.api.meters.MeasurementPublisherFactory
import io.qalipsis.plugins.influxdb.meters.InfluxDbMeasurementConfiguration
import io.qalipsis.plugins.influxdb.meters.InfluxdbMeasurementPublisher
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers

/**
 * Configuration for the export of qalipsis meters to InfluxDb.
 *
 * @author Palina Bril
 */
@Factory
@Requirements(
    Requires(property = MetersConfig.EXPORT_ENABLED, notEquals = StringUtils.FALSE),
    Requires(property = InfluxDbMeterRegistryFactory.INFLUXDB_ENABLED, notEquals = StringUtils.FALSE)
)
internal class InfluxDbMeterRegistryFactory(environment: Environment) : MeasurementPublisherFactory {

    @Singleton
    override fun getPublisher(): InfluxdbMeasurementPublisher {
        return InfluxdbMeasurementPublisher(
            configuration = InfluxDbMeasurementConfiguration(),
            coroutineScope = CoroutineScope(Dispatchers.IO)
        )
    }

    companion object {

        private const val INFLUXDB_CONFIGURATION = "${MetersConfig.EXPORT_CONFIGURATION}.influxdb"

        const val INFLUXDB_ENABLED = "$INFLUXDB_CONFIGURATION.enabled"
    }
}
