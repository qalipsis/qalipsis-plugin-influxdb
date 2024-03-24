package io.qalipsis.plugins.influxdb.meters

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


import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import javax.validation.constraints.Min
import javax.validation.constraints.NotNull

/**
 * Configuration for [InfluxdbMeasurementPublisher].
 *
 * @property url string URL to the Influxdb instance.
 * @property publishers number of concurrent publication of events that can be run defaults to 1 (no concurrency).
 * @property userName name of the user to use for basic authentication when connecting to InfluxDb.
 * @property password password of the user to use for basic authentication when connecting to InfluxDb.
 * @property org name of the organization to use for basic authentication when connecting to InfluxDb.
 * @property bucket password of the bucket to use for saving events to InfluxDb.
 *
 */
@Requires(property = "meters.export.influxdb.enabled", value = "true")
@ConfigurationProperties("meters.export.influxdb")
class InfluxDbMeasurementConfiguration {

    @field:NotNull
    var url: String = "http://localhost:8086"

    @field:Min(1)
    var publishers: Int = 1

    var userName: String = ""

    var password: String = ""

    var org: String = "qalipsis"

    var bucket: String = "qalipsis-meter"

}
