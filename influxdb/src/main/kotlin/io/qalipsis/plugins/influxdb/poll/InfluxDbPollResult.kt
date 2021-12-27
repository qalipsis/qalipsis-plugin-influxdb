package io.qalipsis.plugins.influxdb.poll

data class InfluxDbPollResult(
    val result: Map<String, Any?>
    // TODO Add the meters.
)
