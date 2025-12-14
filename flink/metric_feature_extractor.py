from datetime import datetime, timezone
from spark.utils import (
    normalize_datetime,
    make_json_serializable,
    get_attr_value,
)


class MetricFeatureExtractor:
    """
    Extracts flat, ML-friendly feature records from
    OpenTelemetry Metrics payloads.
    """

    # -------------------------
    # Public API
    # -------------------------

    def extract(self, data: dict, message_ts):
        features = []

        base_ts = normalize_datetime(message_ts) or datetime.now(timezone.utc)

        for rm in data.get("resourceMetrics", []):
            resource_attrs = self._parse_resource_attributes(
                rm.get("resource", {})
            )
            source = self._resolve_source(resource_attrs)

            for sm in rm.get("scopeMetrics", []):
                for metric in sm.get("metrics", []):
                    metric_type, datapoints, aggregation, value_kind = (
                        self._parse_metric_container(metric)
                    )

                    if not datapoints:
                        continue

                    for dp in datapoints:
                        value = self._extract_value(dp)
                        if value is None:
                            continue

                        ts = self._resolve_timestamp(dp, base_ts)

                        feature = {
                            "name": metric.get("name"),
                            "description": metric.get("description"),
                            "unit": metric.get("unit"),
                            "type": metric_type,
                            "value": value,
                            "value_kind": value_kind,
                            "timestamp": ts.isoformat(),
                            "aggregation": aggregation,
                            "attributes": self._parse_attributes(
                                dp.get("attributes", [])
                            ),
                            "resource_attributes": resource_attrs,
                            "source": source,
                            "exemplars_count": len(dp.get("exemplars", [])),
                            "hour": ts.hour,
                            "day_of_week": ts.weekday(),
                        }

                        features.append(
                            make_json_serializable(feature)
                        )

        return features

    # -------------------------
    # Internals
    # -------------------------

    def _parse_resource_attributes(self, resource: dict) -> dict:
        attrs = {}
        for attr in resource.get("attributes", []):
            key = attr.get("key")
            if key:
                attrs[key] = get_attr_value(attr.get("value", {}))
        return attrs

    def _resolve_source(self, resource_attrs: dict) -> str:
        return (
                resource_attrs.get("service.name")
                or resource_attrs.get("service.instance.id")
                or resource_attrs.get("host.name")
                or "unknown"
        )

    def _parse_metric_container(self, metric: dict):
        """
        Returns:
            metric_type (str)
            datapoints (list)
            aggregation (dict)
            value_kind (str)
        """
        if "gauge" in metric:
            return (
                "gauge",
                metric["gauge"].get("dataPoints", []),
                {},
                "gauge",
            )

        if "sum" in metric:
            data = metric["sum"]
            return (
                "sum",
                data.get("dataPoints", []),
                {
                    "temporality": (
                        "cumulative"
                        if data.get("aggregationTemporality") == 2
                        else "delta"
                    ),
                    "is_monotonic": bool(data.get("isMonotonic")),
                },
                "sum",
            )

        if "histogram" in metric:
            data = metric["histogram"]
            return (
                "histogram",
                data.get("dataPoints", []),
                {
                    "temporality": (
                        "cumulative"
                        if data.get("aggregationTemporality") == 2
                        else "delta"
                    )
                },
                "count",
            )

        if "exponentialHistogram" in metric:
            data = metric["exponentialHistogram"]
            return (
                "exponentialHistogram",
                data.get("dataPoints", []),
                {
                    "temporality": (
                        "cumulative"
                        if data.get("aggregationTemporality") == 2
                        else "delta"
                    )
                },
                "count",
            )

        if "summary" in metric:
            return (
                "summary",
                metric["summary"].get("dataPoints", []),
                {},
                "count",
            )

        return None, [], {}, None

    def _extract_value(self, datapoint: dict):
        if "asDouble" in datapoint:
            return float(datapoint["asDouble"])
        if "asInt" in datapoint:
            return float(datapoint["asInt"])
        if "count" in datapoint:
            return float(datapoint["count"])
        return None

    def _parse_attributes(self, attrs: list) -> dict:
        out = {}
        for attr in attrs:
            key = attr.get("key")
            if key:
                out[key] = get_attr_value(attr.get("value", {}))
        return out

    def _resolve_timestamp(self, dp: dict, fallback_ts: datetime) -> datetime:
        ts_nano = dp.get("timeUnixNano")
        if not ts_nano:
            return fallback_ts

        try:
            return datetime.fromtimestamp(
                int(ts_nano) / 1e9, tz=timezone.utc
            )
        except Exception:
            return fallback_ts
