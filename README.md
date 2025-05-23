[![CI](https://github.com/infrasonar/agentcore/workflows/CI/badge.svg)](https://github.com/infrasonar/agentcore/actions)
[![Release Version](https://img.shields.io/github/release/infrasonar/agentcore)](https://github.com/infrasonar/agentcore/releases)

# Infrasonar Agentcore

This is the InfraSonar Agentcore and is used for communication between _hub.infrasonar.com_ and probe collectors.

Variable            | Default               | Description
------------------- | --------------------- | ------------
`TOKEN`             | _required_            | Token for authentication.
`AGENTCORE_NAME`    | _fqdn_                | Name for the Agentcore. If not given, the fqdn is used.
`AGENTCORE_ZONE`    | `0`                   | Zone _(integer)_ for the Agentcore.
`AGENTCORE_DATA`    | `/data`               | Data path _(`.agentcore.json` with the Agentcore Id is also stored in this path)_.
`HUB_HOST`          | `hub.infrasonar.com`  | InfraSonar Hub address.
`HUB_PORT`          | `8730`                | InfraSonar Hub TCP Port to connect to. _(must be either 8730 or 443)_
`PROBE_SERVER_PORT` | `8750`                | Probe connection TCP port.
`RAPP_PORT`         | `8770`                | Remote appliance (RAPP) port.
`LOG_LEVEL`         | `info`                | Log level (`debug`, `info`, `warning`, `error` or `critical`).
`LOG_COLORIZED`     | `1`                   | Log using colors (`0`=disabled, `1`=enabled).
`LOG_FTM`           | `%y%m%d %H:%M:%S`     | Log format prefix.


# Docker image

The agentcore and probe collectors should be pinned to Docker image: `python:3.12.9` 
_(take the pre-latest version, for example when 3.13.2 is current, take 3.13.1 as the current tag might be pushed again)_
