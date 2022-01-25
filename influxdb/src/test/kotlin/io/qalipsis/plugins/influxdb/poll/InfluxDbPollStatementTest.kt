package io.qalipsis.plugins.influxdb.poll

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import io.mockk.spyk
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import java.time.Instant
import org.junit.Test

internal class InfluxDbPollStatementTest {

    @Test
    fun `should not have tie-breaker before the first request`() {
        // given
        val pollStatement = InfluxDbPollStatement("time")

        // when only initialization happens


        // then
        assertThat(pollStatement).prop("tieBreaker").isNull()
    }

   /* @Test
    fun `should reset() clean up tie-breaker`() {
        // given
        val pollStatement = spyk(
            InfluxDbPollStatement("time")
        )

        // when (minor check)
        pollStatement.saveTieBreakerValueForNextPoll(QueryResult())

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
        val pollStatement = spyk(
            InfluxDbPollStatement("time")
        )
        // then
        val actualQuery = pollStatement.convertQueryForNextPoll("SELECT * FROM cpu", InfluxDbPollStepConnectionImpl(), mutableMapOf())
        val expectedQuery = BoundParameterQuery.QueryBuilder.newQuery("SELECT * FROM cpu").forDatabase(InfluxDbPollStepConnectionImpl().bucket)
        assertThat(actualQuery).isEqualTo(expectedQuery)
    }*/
}
