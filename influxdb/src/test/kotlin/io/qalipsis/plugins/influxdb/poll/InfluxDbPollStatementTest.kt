package io.qalipsis.plugins.influxdb.poll

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import com.influxdb.client.domain.Query
import com.influxdb.query.FluxRecord
import io.aerisconsulting.catadioptre.setProperty
import io.mockk.spyk
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.assertk.typedProp
import org.junit.jupiter.api.Test
import java.time.Instant

internal class InfluxDbPollStatementTest {

    @Test
    fun `should not have tie-breaker before the first request`() {
        // given
        val pollStatement = spyk<InfluxDbPollStatement>()

        // when only initialization happens

        // then
        assertThat(pollStatement).prop("tieBreaker").isNull()
    }

    @Test
    fun `should have valid query after first request`() {
        // given
        val pollStatement = spyk<InfluxDbPollStatement>()
        val fluxRecord = FluxRecord(1)
        fluxRecord.setProperty("table", 1)
        val now = Instant.now()
        fluxRecord.values.set("_time", now)
        pollStatement.saveTieBreakerValueForNextPoll(fluxRecord)
        val actualQuery = pollStatement.convertQueryForNextPoll(
            "from(bucket: \"test\"",
            InfluxDbPollStepConnectionImpl(),
            mutableMapOf(),
            mutableListOf(),
            false
        )
        val expectedQuery =
            Query().query("from(bucket: \"test\" |> range(start: $now) |> filter(fn: (r) => r._time >= $now) ")

        // when only initialization happens

        // then
        assertThat(actualQuery).isEqualTo(expectedQuery)
    }

    @Test
    fun `should reset() clean up tie-breaker`() {
        // given
        val pollStatement = spyk<InfluxDbPollStatement>()
        val fluxRecord = FluxRecord(1)
        val now = Instant.now()
        fluxRecord.values.set("_time", now)

        // when (minor check)
        pollStatement.saveTieBreakerValueForNextPoll(fluxRecord)

        // then  (minor check)
        assertThat(pollStatement).typedProp<Instant>("tieBreaker").isNotNull()
        assertThat(pollStatement).typedProp<Instant>("tieBreaker").isEqualTo(now)

        // when (major check)
        pollStatement.reset()

        // then (major check)
        assertThat(pollStatement).prop("tieBreaker").isNull()
    }

    @Test
    fun `should return proper query`() {
        // given
        val pollStatement = spyk<InfluxDbPollStatement>()

        // then
        val actualQuery = pollStatement.convertQueryForNextPoll(
            "from(bucket: \"test\"",
            InfluxDbPollStepConnectionImpl(),
            mutableMapOf(),
            mutableListOf(),
            false
        )
        val expectedQuery = Query().query("from(bucket: \"test\" |> range(start: 0) ")
        assertThat(actualQuery).isEqualTo(expectedQuery)
    }
}
