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

package io.qalipsis.plugins.influxdb.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import io.aerisconsulting.catadioptre.getProperty
import io.aerisconsulting.catadioptre.invokeInvisible
import io.mockk.every
import io.mockk.spyk
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.influxdb.poll.converters.InfluxDbDocumentPollBatchConverter
import io.qalipsis.plugins.influxdb.poll.converters.InfluxDbDocumentPollSingleConverter
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration

/**
 *
 * @author Maxim Golokhov
 */
@WithMockk
internal class InfluxDbPollStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<InfluxDbPollStepSpecificationConverter>() {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<InfluxDbPollStepSpecificationImpl>())).isTrue()
    }

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk())).isFalse()
    }

    @Test
    @ExperimentalCoroutinesApi
    @Timeout(5)
    fun `should convert with name and metrics`() = testDispatcherProvider.runTest {
        // given
        val spec = InfluxDbPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            connect {
                server("http://127.0.0.1:8086", "my-database", "my_org")
                basic("username", "password")
                enableGzip()
            }
            query = "from(bucket: \"test\")"

            monitoring {
                meters = true
                events = false
            }
            pollDelay(Duration.ofSeconds(10L))
            broadcast(123, Duration.ofSeconds(20))
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        val recordsConverter: DatasourceObjectConverter<InfluxDbQueryResult, out Any> = relaxedMockk()
        every { spiedConverter["buildConverter"](refEq(spec)) } returns recordsConverter

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<InfluxDbPollStepSpecificationImpl>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("name").isEqualTo("my-step")
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isSameAs(recordsConverter)
                prop("reader").isNotNull().isInstanceOf(InfluxDbIterativeReader::class).all {
                    prop("pollStatement").isNotNull().isInstanceOf(InfluxDbPollStatement::class).all {
                        prop("query").isEqualTo("from(bucket: \"test\")")
                    }
                    prop("meterRegistry").isEqualTo(meterRegistry)
                    prop("eventsLogger").isNull()
                }
            }
        }
        verifyOnce { spiedConverter["buildConverter"](refEq(spec)) }

        val channelFactory = creationContext.createdStep!!
            .getProperty<InfluxDbIterativeReader>("reader")
            .getProperty<() -> Channel<InfluxDbQueryResult>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    @ExperimentalCoroutinesApi
    @Timeout(5)
    fun `should convert without name and metrics`() = testDispatcherProvider.runTest {
        // given
        val spec = InfluxDbPollStepSpecificationImpl()
        spec.apply {
            this.name = "my-step"
            connect {
                server("http://127.0.0.1:8086", "DB", "org")
                basic("user", "pass")
            }
            query = "from(bucket: \"test\")"

            monitoring {
                meters = false
                events = true
            }
            pollDelay(Duration.ofSeconds(10L))
            broadcast(123, Duration.ofSeconds(20))
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)
        val spiedConverter = spyk(converter, recordPrivateCalls = true)

        val recordsConverter: DatasourceObjectConverter<InfluxDbQueryResult, out Any> = relaxedMockk()
        every { spiedConverter["buildConverter"](refEq(spec)) } returns recordsConverter

        // when
        spiedConverter.convert<Unit, Map<String, *>>(
            creationContext as StepCreationContext<InfluxDbPollStepSpecificationImpl>
        )

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("name").isEqualTo("my-step")
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isSameAs(recordsConverter)
                prop("reader").isNotNull().isInstanceOf(InfluxDbIterativeReader::class).all {
                    prop("pollStatement").isNotNull().isInstanceOf(InfluxDbPollStatement::class).all {
                        prop("query").isEqualTo("from(bucket: \"test\")")
                    }
                    prop("meterRegistry").isNull()
                    prop("eventsLogger").isEqualTo(eventsLogger)
                }
            }
        }
        verifyOnce { spiedConverter["buildConverter"](refEq(spec)) }

        val channelFactory = creationContext.createdStep!!
            .getProperty<InfluxDbIterativeReader>("reader")
            .getProperty<() -> Channel<InfluxDbQueryResult>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    internal fun `should build batch converter`() {
        // given
        val spec = InfluxDbPollStepSpecificationImpl()

        // when
        val converter =
            converter.invokeInvisible<DatasourceObjectConverter<InfluxDbQueryResult, out Any>>("buildConverter", spec)

        // then
        assertThat(converter).isInstanceOf(InfluxDbDocumentPollBatchConverter::class)
    }

    @Test
    internal fun `should build single converter`() {
        // given
        val spec = InfluxDbPollStepSpecificationImpl()
        spec.flattenOutput = true

        // when
        val converter =
            converter.invokeInvisible<DatasourceObjectConverter<InfluxDbQueryResult, out Any>>("buildConverter", spec)

        // then
        assertThat(converter).isInstanceOf(InfluxDbDocumentPollSingleConverter::class)
    }
}
