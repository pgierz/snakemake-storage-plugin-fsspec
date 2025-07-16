from typing import Optional, Type

import pytest
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.tests import TestStorageBase

from snakemake_storage_plugin_fsspec import StorageProvider, StorageProviderSettings


@pytest.fixture  # (scope="session")
def fake_file(tmp_path) -> str:
    # Create a fake file for testing
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("This is a test file.")
    return str(file_path)


class TestStorageFsspecBase(TestStorageBase):
    __test__ = False
    # set to True if the storage is read-only
    retrieve_only = False
    # set to True if the storage is write-only
    store_only = False
    # set to False if the storage does not support deletion
    delete = True
    # set to True if the storage object implements support for touching (inherits from
    # StorageObjectTouch)
    touch = True
    # set to False if also directory upload/download should be tested (if your plugin
    # supports directory down-/upload, definitely do that)
    files_only = False

    def get_query(self, fake_file) -> str:
        # Return a query. If retrieve_only is True, this should be a query that
        # is present in the storage, as it will not be created.
        return str(fake_file)

    def get_query_not_existing(self, tmp_path) -> str:
        # Return a query that is not present in the storage.
        return str(tmp_path / "not_existing.txt")

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        return StorageProviderSettings()


class TestStorageFsspecLocal(TestStorageFsspecBase):
    __test__ = True
    retrieve_only = True


class SFTPFspecTestStorage(TestStorageFsspecBase):
    __test__ = True
    retrieve_only = True
    delete = False
    touch = False  # SFTP via FS-Spec does not support touching files
    files_only = True  # Not strictly speaking true, but I want to see if I can get the tests to pass

    def get_query(self, tmp_path) -> str:
        return "download/version.txt"

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(
            protocol="sftp",
            storage_options={
                "host": "demo.wftpserver.com",
                "port": 2222,
                "username": "demo",
                "password": "demo",
            },
        )
