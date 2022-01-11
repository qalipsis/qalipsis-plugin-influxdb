package io.qalipsis.plugins.influxdb.poll

data class InfluxDbPollResult(
    val result: Map<String, Any?>,
    val meters: InfluxDbQueryMeters
)
