package io.qalipsis.plugins.influxdb.poll

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import com.influxdb.client.domain.Query
import io.mockk.spyk
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import io.qalipsis.test.mockk.relaxedMockk
import java.time.Instant
import org.junit.Test

internal class InfluxDbPollStatementTest {

    @Test
    fun `should not have tie-breaker before the first request`() {
        // given
        val pollStatement = spyk<PollStatement>()

        // when only initialization happens
        // then
        assertThat(pollStatement).prop("tieBreaker").isNull()
    }

    @Test
    fun `should reset() clean up tie-breaker`() {
        // given
        val pollStatement = spyk<PollStatement>()
        // when (minor check)
        pollStatement.saveTieBreakerValueForNextPoll(relaxedMockk())

        // then  (minor check)
        assertThat(pollStatement).typedProp<Instant>("tieBreaker").isNotNull()

        // when (major check)
        pollStatement.reset()

        // then (major check)
        assertThat(pollStatement).prop("tieBreaker").isNull()
    }

    @Test
    fun `should return proper query`() {
        // given
        val pollStatement = spyk<PollStatement>()
        // then
        val actualQuery = pollStatement.convertQueryForNextPoll("from(bucket: \"test\"", InfluxDbPollStepConnectionImpl(), mutableMapOf())
        val expectedQuery = Query().query("from(bucket: \"test\" |> range(start: 0) ")
        assertThat(actualQuery).isEqualTo(expectedQuery)
    }
}
