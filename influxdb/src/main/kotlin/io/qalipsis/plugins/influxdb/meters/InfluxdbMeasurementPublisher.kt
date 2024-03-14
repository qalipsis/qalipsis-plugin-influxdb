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

package io.qalipsis.plugins.influxdb.meters

import com.influxdb.client.InfluxDBClientOptions
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.kotlin.InfluxDBClientKotlin
import com.influxdb.client.kotlin.InfluxDBClientKotlinFactory
import com.influxdb.client.kotlin.WriteKotlinApi
import io.micronaut.context.annotation.Requires
import io.qalipsis.api.Executors
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.meters.MeasurementPublisher
import io.qalipsis.api.meters.MeterSnapshot
import io.qalipsis.api.sync.SuspendedCountLatch
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

/**
 * Implementation of measurement publisher to export meters to influxdb.
 *
 * @author Francisca Eze
 */
@Requires(beans = [InfluxDbMeasurementConfiguration::class])
class InfluxdbMeasurementPublisher(
    private val configuration: InfluxDbMeasurementConfiguration,
    @Named(Executors.BACKGROUND_EXECUTOR_NAME) private val coroutineScope: CoroutineScope,
) : MeasurementPublisher {

    private lateinit var client: InfluxDBClientKotlin

    private lateinit var writeClient: WriteKotlinApi

    private lateinit var publicationLatch: SuspendedCountLatch

    private lateinit var publicationSemaphore: Semaphore

    override suspend fun init() {
        publicationLatch = SuspendedCountLatch(0)
        publicationSemaphore = Semaphore(configuration.publishers)
        buildClient()
    }

    override suspend fun publish(meters: Collection<MeterSnapshot<*>>) {
        publicationLatch.increment()
        coroutineScope.launch {
            try {
                publicationSemaphore.withPermit {
                    performPublish(meters)
                }
            } finally {
                publicationLatch.decrement()
            }
        }
    }

    /**
     * Creates a brand-new client related to the configuration.
     *
     * @see InfluxDbMeasurementConfiguration
     */
    private fun buildClient() {
        client =
            InfluxDBClientKotlinFactory.create(
                InfluxDBClientOptions.builder()
                    .url(configuration.url)
                    .authenticate(
                        configuration.userName,
                        configuration.password.toCharArray()
                    )
                    .org(configuration.org)
                    .build()
            )
        writeClient = client.getWriteKotlinApi()
    }

    private suspend fun performPublish(snapshots: Collection<MeterSnapshot<*>>) {
        val records = snapshots.map(this::createRecord)
        logger.debug { "Exporting ${records.size} meters to InfluxDb" }
        try {
            saveRecords(records)
        } catch (ex: Exception) {
            logger.error(ex) { ex.message }
        }
    }

    private fun createRecord(snapshot: MeterSnapshot<*>): String {
        val meterId = snapshot.meter.id
        val fields = snapshot.measurements.associate { it.statistic.value to it.value }
        val tags = if (meterId.tags.isNotEmpty()) {
            meterId.tags.mapValues { (_, value) -> "\"$value\"" }
                .entries.joinToString(",") + ",metric_type=${meterId.type.value}"
        } else "metric_type=${meterId.type.value}"

        return "${meterId.campaignKey.replaceSpaces()}.${meterId.scenarioName.replaceSpaces()}.${meterId.stepName.replaceSpaces()}.${meterId.meterName.replaceSpaces()},$tags ${
            fields.entries.joinToString(
                ","
            )
        }"
    }


    private suspend fun saveRecords(records: Collection<String>) {
        try {
            writeClient.writeRecords(records, WritePrecision.NS, configuration.bucket, configuration.org)
            logger.debug { "Successfully sent ${records.size} metrics to InfluxDb" }
            logger.trace { "onSuccess totally processed" }
        } catch (ex: Exception) {
            logger.warn(ex) { "${records.size} could not be save: ${ex.message}" }
            throw ex
        }
    }

    override suspend fun stop() {
        logger.debug { "Stopping the meter publication of meters" }
        runBlocking(coroutineScope.coroutineContext) {
            logger.debug { "Waiting for ${publicationLatch.get()} publication jobs to be completed" }
            publicationLatch.await()
        }
        logger.debug { "Closing the InfluxDbKotlin client" }
        tryAndLogOrNull(logger) {
            client.close()
        }
        logger.debug { "The meters publication logger was stopped" }
    }

    private fun String.replaceSpaces() = this.replace(" ", "-").lowercase()

    companion object {
        @JvmStatic
        private val logger = logger()
    }
}