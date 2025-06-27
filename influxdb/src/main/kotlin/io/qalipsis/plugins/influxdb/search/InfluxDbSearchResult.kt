/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.influxdb.search

import com.influxdb.query.FluxRecord
import io.qalipsis.plugins.influxdb.poll.InfluxDbQueryMeters

/**
 * A wrapper for the input for search, meters and documents.
 *
 * @property input input value used to generate the search query
 * @property results result of search query procedure in InfluxDb
 * @property meters meters of the query
 *
 * @author Eric Jessé
 */
class InfluxDbSearchResult<I>(
    val input: I,
    val results: List<FluxRecord>,
    val meters: InfluxDbQueryMeters
)
