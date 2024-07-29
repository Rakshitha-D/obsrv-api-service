import json
import time

from dacite import from_dict
from command.icommand import ICommand
from config import Config
from model.data_models import CommandPayload, DatasetStatusType
from model.db_models import DatasetsLive
from model.telemetry_models import Audit, Object, Property, Transition
from service.db_service import DatabaseService
from service.http_service import HttpService
from service.telemetry_service import TelemetryService

class DatasetCommand(ICommand):
    def __init__(
        self,
        db_service: DatabaseService,
        telemetry_service: TelemetryService,
        http_service: HttpService,
        config: Config,
    ):
        self.db_service = db_service
        self.telemetry_service = telemetry_service
        self.http_service = http_service
        self.config = config
        self.http_service = http_service
        self.config_service_host = self.config.find("config_service.host")
        self.config_service_port = self.config.find("config_service.port")
        self.base_url = f"http://{self.config_service_host}:{self.config_service_port}/datasets/v1/export"

    def _get_draft_dataset_record(self, dataset_id):
        query = f"""
            SELECT "type", MAX(version) AS max_version FROM datasets_draft WHERE dataset_id = '{dataset_id}' GROUP BY 1
        """
        dataset_record = self.db_service.execute_select_one(query)
        if dataset_record is not None:
            return dataset_record
        return None
    
    def _get_draft_dataset(self, dataset_id):
        query = f"""
            SELECT * FROM datasets_draft
            WHERE dataset_id = '{dataset_id}' AND (status = '{DatasetStatusType.Publish.name}' OR status = '{DatasetStatusType.ReadyToPublish.name}') AND version = (SELECT MAX(version)
            FROM datasets_draft WHERE dataset_id = '{dataset_id}' AND (status = '{DatasetStatusType.Publish.name}' OR status = '{DatasetStatusType.ReadyToPublish.name}'))
            """
        dataset_record = self.db_service.execute_select_one(query)
        if dataset_record is not None:
            return dataset_record
        return None

    def _check_for_live_record(self, dataset_id):
        query = f"""
            SELECT * FROM datasets WHERE dataset_id = '{dataset_id}' AND status = '{DatasetStatusType.Live.name}'
        """
        result = self.db_service.execute_select_one(query)
        live_dataset = dict()
        if result is not None:
            live_dataset = from_dict(data_class=DatasetsLive, data=result)
            data_version = live_dataset.data_version + 1
            return live_dataset, data_version
        return None, None

    def audit_live_dataset(self, command_payload: CommandPayload, ts: int):
        dataset_id = command_payload.dataset_id
        dataset_record, data_version = self._check_for_live_record(dataset_id)
        export_dataset = self.http_service.post(
            url=self.base_url,
            body=json.dumps({"dataset_id": dataset_id}),
            headers={"Content-Type": "application/json"},
        )
        if export_dataset.status == 200:
            result = json.loads(export_dataset.body)
            object_ = Object(
                dataset_id, dataset_record.type, dataset_record.data_version
            )
            live_dataset_property = Property("dataset:export", result["result"], "")
            draft_property = Property(
                "draft-dataset:status",
                DatasetStatusType.ReadyToPublish.name,
                DatasetStatusType.Live.name,
            )
            dataset_property = Property(
                "dataset:status",
                DatasetStatusType.Live.name,
                DatasetStatusType.Live.name,
            )
            transition = Transition(duration=int(time.time() - ts))
            edata = Audit(
                props=[
                    draft_property,
                    dataset_property,
                    live_dataset_property,
                ],
                transition=transition,
            )
            self.telemetry_service.audit(object_=object_, edata=edata)
            return True
        else:
            print(
                "Failed to get dataset configurations from export API, dataset_id: ",
                dataset_id,
            )
            print("Dataset record details: ", json.dumps(dataset_record))
            return False
