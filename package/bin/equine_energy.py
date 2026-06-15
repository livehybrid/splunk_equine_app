"""
Equine Energy Usage modular input.
Collects usage data from the Equine Energy API (getUsage) and writes events.
"""
import import_declare_test  # noqa: F401  (UCC: must be first; fixes sys.path for bundled libs)

import json
import sys

from splunklib import modularinput as smi
from solnlib import conf_manager
from solnlib import log
from solnlib.modular_input import checkpointer

import requests

APP_NAME = "splunk_app"
CONF_NAME = "splunk_app_account"
SETTINGS_CONF = "splunk_app_settings"
SOURCETYPE = "equine:energy:usage"


def _logger(input_name):
    logger = log.Logs().get_logger(f"{APP_NAME}_equine_energy")
    return logger


def _get_account_config(session_key, account_name, logger):
    """Read the encrypted account stanza (api_url, api_key)."""
    cfm = conf_manager.ConfManager(
        session_key,
        APP_NAME,
        realm=f"__REST_CREDENTIAL__#{APP_NAME}#configs/conf-{CONF_NAME}",
    )
    account_conf = cfm.get_conf(CONF_NAME).get(account_name)
    return account_conf


def _get_proxy_settings(session_key, logger):
    """Read proxy settings from the settings conf, if enabled."""
    try:
        cfm = conf_manager.ConfManager(session_key, APP_NAME)
        settings = cfm.get_conf(SETTINGS_CONF).get("proxy", only_current_app=True)
    except Exception:
        return None

    if not settings or str(settings.get("proxy_enabled", "0")) not in ("1", "true", "True"):
        return None

    proxy_type = settings.get("proxy_type", "http")
    host = settings.get("proxy_url")
    port = settings.get("proxy_port")
    if not host or not port:
        return None

    user = settings.get("proxy_username")
    pwd = settings.get("proxy_password")
    if user and pwd:
        auth = f"{user}:{pwd}@"
    else:
        auth = ""
    proxy_uri = f"{proxy_type}://{auth}{host}:{port}"
    return {"http": proxy_uri, "https": proxy_uri}


class EquineEnergyInput(smi.Script):
    def __init__(self):
        super(EquineEnergyInput, self).__init__()

    def get_scheme(self):
        scheme = smi.Scheme("equine_energy")
        scheme.description = "Equine Energy Usage"
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False

        arg = smi.Argument("account")
        arg.title = "Account"
        arg.description = "The Equine Energy account to use for this input."
        arg.required_on_create = True
        scheme.add_argument(arg)

        arg = smi.Argument("interval")
        arg.title = "Interval"
        arg.description = "Polling interval in seconds (or a cron schedule)."
        arg.required_on_create = True
        scheme.add_argument(arg)

        arg = smi.Argument("index")
        arg.title = "Index"
        arg.description = "Index"
        arg.required_on_create = True
        scheme.add_argument(arg)

        return scheme

    def validate_input(self, validation_definition):
        # ucc-gen / globalConfig validators handle field-level validation.
        pass

    def stream_events(self, inputs, ew):
        for input_name, input_item in inputs.inputs.items():
            normalized_name = input_name.split("://")[-1]
            logger = _logger(normalized_name)
            session_key = self._input_definition.metadata["session_key"]

            try:
                log.modular_input_start(logger, normalized_name)

                account_name = input_item.get("account")
                index = input_item.get("index")

                account = _get_account_config(session_key, account_name, logger)
                if not account:
                    logger.error("Account '%s' not found.", account_name)
                    continue

                api_url = account.get("api_url")
                api_key = account.get("api_key")
                if not api_url or not api_key:
                    logger.error("Account '%s' is missing api_url or api_key.", account_name)
                    continue

                proxies = _get_proxy_settings(session_key, logger)

                # Optional checkpoint to avoid re-ingesting the same window.
                ckpt = checkpointer.KVStoreCheckpointer(
                    f"{APP_NAME}_equine_energy_checkpoint",
                    session_key,
                    APP_NAME,
                )
                last_ts = ckpt.get(normalized_name)

                headers = {
                    "x-api-key": api_key,
                    "Accept": "application/json",
                }
                params = {}
                if last_ts:
                    params["since"] = last_ts

                logger.info("Requesting data from %s", api_url)
                resp = requests.get(
                    api_url,
                    headers=headers,
                    params=params or None,
                    proxies=proxies,
                    timeout=60,
                )
                resp.raise_for_status()

                try:
                    payload = resp.json()
                except ValueError:
                    logger.error("Response was not valid JSON.")
                    continue

                # The API may return a single object or a list of records.
                if isinstance(payload, dict):
                    records = payload.get("data") or payload.get("usage") or [payload]
                    if not isinstance(records, list):
                        records = [records]
                elif isinstance(payload, list):
                    records = payload
                else:
                    records = [payload]

                count = 0
                newest_ts = last_ts
                for record in records:
                    event = smi.Event()
                    event.stanza = input_name
                    event.sourceType = SOURCETYPE
                    if index:
                        event.index = index
                    event.data = json.dumps(record, ensure_ascii=False)
                    ew.write_event(event)
                    count += 1

                    if isinstance(record, dict):
                        ts = record.get("timestamp") or record.get("time")
                        if ts and (newest_ts is None or str(ts) > str(newest_ts)):
                            newest_ts = ts

                if newest_ts and newest_ts != last_ts:
                    ckpt.update(normalized_name, newest_ts)

                logger.info("Indexed %d event(s) for input '%s'.", count, normalized_name)
                log.modular_input_end(logger, normalized_name)

            except requests.exceptions.HTTPError as e:
                logger.error("HTTP error from Equine Energy API: %s", e)
            except requests.exceptions.RequestException as e:
                logger.error("Request to Equine Energy API failed: %s", e)
            except Exception as e:
                logger.exception("Unexpected error collecting data: %s", e)


if __name__ == "__main__":
    exit_code = EquineEnergyInput().run(sys.argv)
    sys.exit(exit_code)
