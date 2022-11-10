# agentcore

Variable            | Default                       | Description
------------------- | ----------------------------- | ------------
`TOKEN`             | _required_                    | Token for authentication.
`AGENTCORE_NAME`    | _fqdn_                        | Name for the Agentcore. If not given, the fqdn is used.
`AGENTCORE_ZONE`    | `0`                           | Zone _(integer)_ for the Agentcore.
`AGENTCORE_JSON`    | `/data/.agentcore.json`       | JSON file where the Agentcore Id is stored.
`HUB_HOST`          | `hub.infrasonar.com`          | InfraSonar Hub address.
`HUB_PORT`          | `8730`                        | InfraSonar Hub TCP Port to connect to. _(should be the default 8730 for InfraSonar)_
`PROBE_SERVER_PORT` | `8750`                        | Probe connection TCP port.
`LOG_LEVEL`         | `warning`                     | Log level (`debug`, `info`, `warning`, `error` or `critical`).
`LOG_COLORIZED`     | `0`                           | Log using colors (`0`=disabled, `1`=enabled).
`LOG_FTM`           | `%y%m%d %H:%M:%S`             | Log format prefix.
