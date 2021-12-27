package io.qalipsis.plugins.influxdb.poll

data class InfluxDbPollResults(
    val results: List<Map<String, Any?>>
    // TODO Add the meters.
) : Iterable<InfluxDbPollResult> {

    override fun iterator(): Iterator<InfluxDbPollResult> {
        TODO("Not yet implemented")
    }
}
