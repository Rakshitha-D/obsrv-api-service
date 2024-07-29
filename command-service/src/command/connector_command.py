from dacite import from_dict
import json
import logging
import subprocess

from command.icommand import ICommand
from config import Config
from model.data_models import Action, ActionResponse, CommandPayload, DatasetStatusType
from model.db_models import ConnectorInstance
from service.db_service import DatabaseService


class ConnectorCommand(ICommand):
    def __init__(self, config: Config, db_service: DatabaseService):
        self.config = config
        self.db_service = db_service
        self.logger = logging.getLogger()
        self.connector_job_config = self.config.find("connector_jobs")

    def execute(self, command_payload: CommandPayload, action: Action):
        result = None
        active_connectors = self._get_connector_details(
            dataset_id=command_payload.dataset_id
        )
        is_masterdata = self._get_masterdata_details(
            dataset_id=command_payload.dataset_id
        )

        print(f"Active connectors: {active_connectors}")
        print(f"Is masterdata: {is_masterdata}")

        if action == Action.DEPLOY_CONNECTORS.name:
            result = self._deploy_connectors(
                command_payload.dataset_id, active_connectors, is_masterdata
            )

        return result

    def _deploy_connectors(self, dataset_id, active_connectors, is_masterdata):
        result = None
        # self._stop_connector_jobs(dataset_id, active_connectors, is_masterdata)
        result = self._install_jobs(dataset_id, active_connectors, is_masterdata)

        return result

    def _stop_connector_jobs(self, dataset_id, active_connectors, is_masterdata):
        managed_releases = []
        connector_jar_config = self.config.find("connector_job")
        masterdata_jar_config = self.config.find("masterdata_job")
        for connector_type in connector_jar_config:
            for release in connector_jar_config[connector_type]:
                managed_releases.append(release["release_name"])
        if is_masterdata:
            for release in masterdata_jar_config:
                managed_releases.append(release["release_name"])

        helm_ls_cmd = ["helm", "ls", "--namespace", self.connector_job_ns]
        helm_ls_result = subprocess.run(
            helm_ls_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        if helm_ls_result.returncode == 0:
            jobs = helm_ls_result.stdout.decode()
            for job in jobs.splitlines()[1:]:
                release_name = job.split()[0]
                if release_name in managed_releases:
                    print("Uninstalling job {0}".format(release_name))
                    helm_uninstall_cmd = [
                        "helm",
                        "uninstall",
                        release_name,
                        "--namespace",
                        self.connector_job_ns,
                    ]
                    helm_uninstall_result = subprocess.run(
                        helm_uninstall_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    if helm_uninstall_result.returncode == 0:
                        print(f"Successfully uninstalled job {release_name}...")
                    else:
                        print(
                            f"Error uninstalling job {release_name}: {helm_uninstall_result.stderr.decode()}"
                        )

    def _install_jobs(self, dataset_id, active_connectors, is_masterdata):
        result = None

        for connector in active_connectors:
            print("Installing connector {0}".format(connector))

            if connector.connector_runtime == "spark":
                result = self._perform_spark_install(connector)
            else:
                print(
                    f"Connector {connector.connector_id} is not supported for deployment"
                )
                break

        # if is_masterdata:
        #     print("Installing masterdata job")
        #     masterdata_jar_config = self.config.find("masterdata_job")
        #     for release in masterdata_jar_config:
        #         result = self._perform_install(release)
        return result

    def _perform_spark_install(self, connector_instance):
        err = None
        result = None
        release_name = connector_instance.id
        # print("Instance -->>", connector_instance)
        connector_source = json.loads(connector_instance.connector_source)
        print(connector_source)
        helm_install_cmd = [
            "helm",
            "upgrade",
            "--install",
            release_name,
            f"""{self.config.find("helm_charts_base_dir")}/{self.connector_job_config["spark"]["base_helm_chart"]}""",
            "--namespace",
            self.connector_job_config["spark"]["namespace"],
            "--create-namespace",
            "--set",
            "technology={}".format(connector_instance.technology),
            "--set",
            "instance_id={}".format(release_name),
            "--set",
            "connector_source={}".format(connector_source["source"]),
            "--set",
            "main_class={}".format(connector_source["main_class"]),
            "--set",
            "main_file={}".format(connector_source["main_program"]),
            "--set",
            "cronSchedule={}".format(connector_instance.operations_config["schedule"])
        ]

        print(" ".join(helm_install_cmd))

        helm_install_result = subprocess.run(
            helm_install_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if helm_install_result.returncode == 0:
            print(f"Job {release_name} deployment succeeded...")
        else:
            err = True
            result = ActionResponse(
                status="ERROR",
                status_code=500,
                error_message="SPARK_CRON_HELM_INSTALLATION_EXCEPTION",
            )
            print(
                f"Error re-installing job {release_name}: {helm_install_result.stderr.decode()}"
            )

        if err is None:
            result = ActionResponse(status="OK", status_code=200)

        return result

    def _get_connector_details(self, dataset_id):
        active_connectors = []
        records = self.db_service.execute_select_all(
            f"""
                SELECT ci.id, ci.connector_id, ci.operations_config, cr.runtime as connector_runtime, cr.source as connector_source, cr.technology
                FROM connector_instances ci
                JOIN connector_registry cr on ci.connector_id = cr.id
                WHERE ci.status='{DatasetStatusType.Live.name}' and ci.dataset_id = '{dataset_id}'
            """
        )

        for record in records:
            active_connectors.append(from_dict(
                data_class=ConnectorInstance, data=record
            ))

        return active_connectors

    def _get_masterdata_details(self, dataset_id):
        is_masterdata = False
        rows = self.db_service.execute_select_all(
            f"""
                SELECT *
                FROM datasets
                WHERE status='{DatasetStatusType.Live.name}' AND dataset_id = '{dataset_id}' AND type = 'master'
            """
        )

        if len(rows) > 0:
            is_masterdata = True

        return is_masterdata
