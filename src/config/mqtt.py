"""MQTT broker connection settings.

The defaults match the ``mqtt`` service in ``compose.yaml`` (eclipse-
mosquitto, no auth, port 1883). For a real edge deployment point
``MQTT_BROKER_URL`` at the production broker and fill in the optional
``MQTT_USERNAME`` / ``MQTT_PASSWORD`` fields.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MqttSettings(BaseSettings):
    """Broker connection settings, read from ``stack.env``.

    ``MQTT_BROKER_URL`` accepts both ``tcp://`` (default, plain MQTT)
    and ``mqtts://`` (TLS). The ``MQTT_TLS_INSECURE`` flag disables
    certificate verification when running against a self-signed
    broker in a lab environment.
    """

    model_config = SettingsConfigDict(
        env_file="stack.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    broker_url: str = Field(
        default="mqtt://mosquitto:1883",
        validation_alias="MQTT_BROKER_URL",
    )
    client_id: str = Field(
        default="agro-tracking",
        validation_alias="MQTT_CLIENT_ID",
    )
    username: str | None = Field(
        default=None,
        validation_alias="MQTT_USERNAME",
    )
    password: str | None = Field(
        default=None,
        validation_alias="MQTT_PASSWORD",
    )
    keepalive: int = Field(
        default=60,
        validation_alias="MQTT_KEEPALIVE",
    )
    qos: int = Field(
        default=1,
        validation_alias="MQTT_QOS",
    )
    tls_insecure: bool = Field(
        default=False,
        validation_alias="MQTT_TLS_INSECURE",
    )
    reconnect_initial_delay: float = Field(
        default=1.0,
        validation_alias="MQTT_RECONNECT_INITIAL_DELAY",
    )
    reconnect_max_delay: float = Field(
        default=30.0,
        validation_alias="MQTT_RECONNECT_MAX_DELAY",
    )

    # Edge-side topic that the consumer listens to.
    detection_topic: str = Field(
        default="farm/+/detections",
        validation_alias="MQTT_DETECTION_TOPIC",
    )


mqtt_settings = MqttSettings()
