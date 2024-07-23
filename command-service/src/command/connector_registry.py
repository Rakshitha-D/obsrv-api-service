import json
import os
import tarfile
import uuid
import zipfile
from datetime import datetime

import requests
from fastapi import status

from config import Config
from service.http_service import HttpService
from service.db_service import DatabaseService
from model.db_models import ConnectorRegsitryv2


class RegistryResponse:
    def __init__(self, status: str, message: str, statusCode: status, connector_info = None):
        self.status = status
        self.connector_info = connector_info
        self.message = message  # Ensure message is directly a string
        self.statusCode = statusCode


class ConnectorRegistry:
    def __init__(self):
        self.metadata = dict()
        self.db_service = DatabaseService()
        self.config = Config()
        self.download_path = self.config.find("connector_registry.download_path")
        self.uuid = str(uuid.uuid4())
        self.extraction_path = os.path.join(self.download_path, self.uuid)
        self.metadata = None
        self.ui_spec = None
        self.metadata_file_name = self.config.find(
            "connector_registry.metadata_file_name"
        )
        self.ui_spec_file_name = self.config.find("connector_registry.ui_spec_file")
        self.dataset_api_url = self.config.find("dataset_api.host").strip("/")
        self.pre_signed_url = self.config.find("dataset_api.pre_signed_url").strip("/")

    def register(self, rel_path: str) -> RegistryResponse:
        try:
            download_file_path = os.path.join(self.download_path, rel_path)
            file_extension = rel_path.split(".")[-1]

            # if not os.path.exists(download_file_path):
            http_service = HttpService()
            print(
                f"Connector Registry | Received request to register connector: {rel_path} | {self.dataset_api_url}/{self.pre_signed_url}"
            )
            dataset_api_request = {"request": {"files": [rel_path], "access": "read", "type": "connector"}}
            dataset_api_response = http_service.post(
                url=f"{self.dataset_api_url}/{self.pre_signed_url}",
                    body=json.dumps(dataset_api_request),
                    headers={"Content-Type": "application/json"}
                )
            print(
                f"Connector Registry | Dataset API Response: {dataset_api_response.body}"
            )

            if dataset_api_response.status != 200:
                return RegistryResponse(
                    status="failure",
                    message="dataset api failed to generate read url.",
                    statusCode=status.HTTP_400_BAD_REQUEST,
                )

            dataset_api_response_data = json.loads(dataset_api_response.body)
            url = dataset_api_response_data.get("result", [{}])[0].get(
                "preSignedUrl", None
            )
            if not url:
                return RegistryResponse(
                    status="failure",
                    message="dataset api failed to generate read url.",
                    statusCode=status.HTTP_400_BAD_REQUEST,
                )

            self.cleanup_download_path()
            os.makedirs(self.download_path, exist_ok=True)

            # download the file
            download_status = self.download_file(url, download_file_path)
            print(f"Connector Registry | Download status: {download_status}")
            if not download_status:
                return RegistryResponse(
                    status="failure",
                    message="failed to download the file",
                    statusCode=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            # extract the file
            os.makedirs(self.extraction_path, exist_ok=True)
            extraction_status = ExtractionUtil.extract(
                file=download_file_path,
                extract_out_path=self.extraction_path,
                ext=file_extension,
            )
            if extraction_status.status.lower() == "failure":
                return extraction_status

            load_metadata_status = self.load_metadata(self.extraction_path)

            # Loading of metadata
            if not load_metadata_status:
                return RegistryResponse(
                    status="failure",
                    message="unable to locate the metadata file",
                    statusCode=status.HTTP_422_UNPROCESSABLE_ENTITY,
                )

            # check if folder exists
            connector_source = f"{self.metadata['metadata']['id']}-{self.metadata['metadata']['version']}"
            if not os.path.exists(os.path.join(self.extraction_path, connector_source)):
                return RegistryResponse(
                    status="failure",
                    message=f"connector source folder not found; expecting {connector_source} folder inside the archive",
                    statusCode=status.HTTP_422_UNPROCESSABLE_ENTITY,
                )

            load_ui_config_status = self.load_ui_metadata(self.extraction_path)

            # Loading of ui configurations
            if not load_ui_config_status:
                return RegistryResponse(
                    status="failure",
                    message="Unable to locate the ui configuration File",
                    statusCode=status.HTTP_422_UNPROCESSABLE_ENTITY,
                )

            # Process of the metadata
            return self.process_metadata(rel_path, connector_source)

        except Exception as e:
            print(f"Connector Registry | An error occurred: {e}")
            return RegistryResponse(
                status="failure",
                message="Failed to register connector",
                statusCode=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    # Method to clean up the local directory
    def cleanup_download_path(self):
        if os.path.exists(self.download_path):
            os.system(f"rm -rf {self.download_path}/*")

    # Method to load the file data into object (ex: ui config file and metadata file)
    def load_json_file(self, extract_out_path, file_name, attribute_name):
        for root, dirs, files in os.walk(extract_out_path):
            for name in files:
                if name == file_name:
                    print(f"Connector Registry | {file_name} found")
                    with open(os.path.join(root, name)) as f:
                        data = json.load(f)
                        if not data:
                            print(f"Connector Registry | Empty {file_name} file")
                            return False
                        setattr(self, attribute_name, data)
                        return True
        print(f"Connector Registry | {file_name} not found")
        return False

    def load_metadata(self, extract_out_path):
        return self.load_json_file(
            extract_out_path, self.metadata_file_name, "metadata"
        )

    def load_ui_metadata(self, extract_out_path):
        return self.load_json_file(extract_out_path, self.ui_spec_file_name, "ui_spec")

    # Method to save the metadata and ui_spec configuration into db
    def process_metadata(self, rel_path, connector_source) -> RegistryResponse:
        result = []
        tenant = self.metadata.get("metadata", {}).get("tenant", "")

        if tenant == "multiple":
            connector_objects = self.metadata["connectors"]
            for obj in connector_objects:
                source = {
                    "source": connector_source,
                    "main_class": obj["main_class"] if "main_class" in obj else self.metadata["metadata"]["main_class"],
                    "main_program": obj["main_program"] if "main_program" in obj else self.metadata["metadata"]["main_program"],
                }

                connector_id = obj["id"].replace(" ", "-")
                registry_meta = ConnectorRegsitryv2(connector_id,
                        obj['name'],
                        'source',
                        self.metadata['metadata']['category'],
                        self.metadata['metadata']['version'],
                        obj['description'],
                        self.metadata['metadata']['technology'],
                        self.metadata['metadata']['runtime'],
                        self.metadata['metadata']['licence'],
                        self.metadata['metadata']['owner'],
                        obj['icon'],
                        'Live',
                        rel_path,
                        json.dumps(source),
                        'SYSTEM',
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        self.ui_spec[obj["id"]] if obj["id"] in self.ui_spec else {},
                        'SYSTEM',
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                query = self.build_insert_query(registry_meta)
                success = self.execute_query(query)
                if not success:
                    return RegistryResponse(
                        status="failure",
                        message=f"Failed to register connector {connector_id}",
                        statusCode=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    )
                result.append(registry_meta.to_dict())    
            return RegistryResponse(
                status="success",
                connector_info=result,
                message="Connectors registered successfully",
                statusCode=status.HTTP_200_OK
            )

        else:
            connector_id = (
                self.metadata.get("metadata", {}).get("id", "").replace(" ", "-")
            )

            source = {
                "source": connector_source,
                "main_class": self.metadata["metadata"]["main_class"],
                "main_program": self.metadata["metadata"]["main_program"],
            }

            registry_meta = ConnectorRegsitryv2(
                connector_id,
                self.metadata['metadata']['name'],
                'source',
                self.metadata['metadata']['category'],
                self.metadata['metadata']['version'],
                self.metadata['metadata']['description'],
                self.metadata['metadata']['technology'],
                self.metadata['metadata']['runtime'],
                self.metadata['metadata']['licence'],
                self.metadata['metadata']['owner'],
                self.metadata['metadata']['icon'],
                'Live',
                rel_path,
                json.dumps(source),
                'SYSTEM',
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                self.ui_spec,
                'SYSTEM',
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            query = self.build_insert_query(registry_meta)
            success = self.execute_query(query)
            if not success:
                return RegistryResponse(
                    status="failure",
                    message=f"Failed to register connector {connector_id}",
                    statusCode=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
            return RegistryResponse(
                status="success",
                message="Connectors registered successfully",
                connector_info=[registry_meta.to_dict()],
                statusCode=status.HTTP_200_OK,
            )

    def execute_query(self, query) -> bool:
        try:
            result = self.db_service.execute_upsert(query)
            return result > 0  # Assuming the result is the number of affected rows
        except Exception as e:
            print(
                f"Connector Registry | An error occurred during the execution of Query: {e}"
            )
            return False

    # Method to download the file from blob store
    def download_file(self, url, destination) -> bool:
        try:
            print(f"destination {destination}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(destination, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(
                f"Connector Registry | Download completed successfully. URL:{url} Destination: {destination}"
            )
            return True
        except requests.exceptions.HTTPError as http_err:
            print(
                f"Connector Registry | HTTP error occurred during the file download:  {http_err}"
            )
            return False
        except Exception as e:
            print(
                f"Connector Registry | An unexpected error occurred during the file download:  {e}"
            )
            return False

    def build_insert_query(self, registry_meta: ConnectorRegsitryv2):
        ui_spec_json = json.dumps(registry_meta.ui_spec)
        return f""" INSERT INTO connector_registry (id, connector_id, name, type, category, version, description, technology, runtime, licence, owner, iconurl, status, source_url, source, ui_spec, created_by, updated_by, created_date, updated_date, live_date) VALUES
        ( '{registry_meta.id}-{registry_meta.version}', '{registry_meta.id}', '{registry_meta.name}', '{registry_meta.type}', '{registry_meta.category}', '{registry_meta.version}', '{registry_meta.description}', '{registry_meta.technology}', '{registry_meta.runtime}', '{registry_meta.licence}', '{registry_meta.owner}', '{registry_meta.iconurl}', '{registry_meta.status}', '{registry_meta.source_url}', '{registry_meta.source}', '{ui_spec_json}', 'SYSTEM', 'SYSTEM', '{datetime.now()}', '{datetime.now()}', '{datetime.now()}' )
            ON CONFLICT (connector_id, version) DO UPDATE
            SET id = '{registry_meta.id}-{registry_meta.version}',
                name = '{registry_meta.name}',
                type = '{registry_meta.type}',
                category = '{registry_meta.category}',
                version = '{registry_meta.version}',
                description = '{registry_meta.description}',
                technology = '{registry_meta.technology}',
                runtime = '{registry_meta.runtime}',
                licence = '{registry_meta.licence}',
                owner = '{registry_meta.owner}',
                iconurl = '{registry_meta.iconurl}',
                status = '{registry_meta.status}',
                source_url = '{registry_meta.source_url}',
                source = '{registry_meta.source}',
                ui_spec = '{ui_spec_json}'::jsonb,
                updated_date = '{datetime.now()}'
                ;;
        """


class ExtractionUtil:
    def extract_gz(tar_path, extract_path):
        with tarfile.open(tar_path, "r:*") as tar:
            tar.extractall(path=extract_path)

    def extract_zip(tar_path, extract_path):
        with zipfile.ZipFile(tar_path, "r") as zip_ref:
            zip_ref.extractall(path=extract_path)

    # Method to extract the compressed files
    def extract(file, extract_out_path, ext) -> RegistryResponse:
        extraction_function = ExtractionUtil.extract_gz

        compression_types = {
            "zip": ExtractionUtil.extract_zip,
        }

        try:
            print(
                f"Connector Registry | Extracting {file} to {extract_out_path} of {ext} file type"
            )

            if ext in compression_types:
                extraction_function = compression_types.get(ext)

            extraction_function(file, extract_out_path)
            print(f"Connector Registry | Extraction complete for {file}")
            return RegistryResponse(
                status="success",
                message="Extraction Successfully Completed",
                statusCode=status.HTTP_200_OK,
            )
        except (tarfile.TarError, zipfile.BadZipFile, OSError) as e:
            print(
                f"Connector Registry | An error occurred while extracting the file: {e}"
            )
            return RegistryResponse(
                status="failure",
                message="Failed to Extract the File",
                statusCode=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        except Exception as e:
            print(f"Connector Registry | An unexpected error occurred: {e}")
            return RegistryResponse(
                status="failure",
                message="Failed to Extract the File",
                statusCode=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
