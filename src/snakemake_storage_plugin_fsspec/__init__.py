import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, List, Optional

import fsspec
from fsspec import AbstractFileSystem

# Raise errors that will not be handled within this plugin but thrown upwards to
# Snakemake and the user as WorkflowError.
from snakemake_interface_common.exceptions import WorkflowError  # noqa: F401
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectGlob,
    StorageObjectRead,
    StorageObjectTouch,
    StorageObjectWrite,
    retry_decorator,
)
from snakemake_interface_storage_plugins.storage_provider import (  # noqa: F401
    ExampleQuery,
    Operation,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)


def _parse_protocol_setting(setting):
    if setting not in fsspec.available_protocols():
        raise ValueError(f"You must use one of {fsspec.available_protocols()}")
    return setting


# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    protocol: Optional[str] = field(
        default="file",
        metadata={
            "help": "The fsspec protocol to use for this storage provider",
            "env_var": False,
            "parse_func": _parse_protocol_setting,
            # [NOTE] No unparsing, just placeholder.
            "unparse_func": lambda x: x,
            "required": False,
        },
    )
    storage_options: Optional[dict] = field(
        default=None,
        metadata={
            "help": "Additional keyword arguments to pass to the filesystem class",
            "env_var": False,
        },
    )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
# Inside of the provider, you can use self.logger (a normal Python logger of type
# logging.Logger) to log any additional information or
# warnings.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # further stuff.

    def __post_init__(self):
        # Initialize the filesystem based on the protocol
        self.logger.debug(
            f"Initializing FileSystem abstraction for storage provider: {self.settings.protocol}"
        )
        self._fs = fsspec.filesystem(
            self.settings.protocol,
            **(self.settings.storage_options or {}),
        )

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                query="myfile.txt",
                type=QueryType.ANY,
                description="A file on a local server.",
            ),
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        return super().rate_limiter_key(query, operation)

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return super().default_max_requests_per_second()

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return super().use_rate_limiter()

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        # [FIXME] This needs some logic, right now everything is always valid...
        return StorageQueryValidationResult(query=query, valid=True)

    # If required, overwrite the method postprocess_query from StorageProviderBase
    # in order to e.g. normalize the query or add information from the settings to it.
    # Otherwise, remove this method as it will be inherited from the base class.
    def postprocess_query(self, query: str) -> str:
        return super().postprocess_query(query)

    # This can be used to change how the rendered query is displayed in the logs to
    # prevent accidentally printing sensitive information e.g. tokens in a URL.
    def safe_print(self, query: str) -> str:
        """Process the query to remove potentially sensitive information when printing."""
        return super().safe_print(query)


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
# Inside of the object, you can use self.provider to access the provider (e.g. for )
# self.provider.logger, see above, or self.provider.settings).
class StorageObject(
    StorageObjectRead, StorageObjectWrite, StorageObjectGlob, StorageObjectTouch
):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # further stuff.

    def __post_init__(self):
        # Initialize the fsspec filesystem from the provider
        self._fs: AbstractFileSystem = self.provider._fs

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        pass
        # [FIXME] Not sure how to exactly implement this, since the Cache interface is
        #         poorly documented.
        # try:
        #     info = self._fs.info(self.query)
        #     cache.set(
        #         self.cache_key(),
        #         {"exists": True, "mtime": info["mtime"], "size": info["size"]},
        #     )
        # except FileNotFoundError:
        #     cache.set(self.cache_key(), {"exists": False})

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        pass
        # [FIXME] Not sure how to exactly implement this, since the Cache interface is
        #         poorly documented.
        # try:
        #     return str(self._fs._parent(self.query))
        # except Exception:
        #     return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path of the object."""
        # For local paths, use the path as is
        if self.query.startswith(("file://", "/", "./", "../")):
            path = Path(self.query.replace("file://", ""))
            if path.is_absolute():
                parts = path.parts[1:]  # Remove leading slash for absolute paths
                return str(Path(*parts))
            return str(path)

        # For remote paths, use a hash of the full query to ensure uniqueness
        # and avoid path traversal issues
        import hashlib

        query_hash = hashlib.md5(self.query.encode("utf-8")).hexdigest()
        return f"remote/{query_hash}/{Path(self.query).name}"

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        pass

    @retry_decorator
    def exists(self) -> bool:
        """Check if the object exists in the storage."""
        return bool(self._fs.exists(self.query))

    @retry_decorator
    def mtime(self) -> float:
        """Get the modification time of the object."""
        info = self._fs.info(self.query)
        if isinstance(info["mtime"], datetime.datetime):
            return info["mtime"].timestamp()
        return float(info["mtime"])

    @retry_decorator
    def size(self) -> int:
        """Get the size of the object in bytes."""
        info = self._fs.info(self.query)
        return int(info["size"])

    @retry_decorator
    def retrieve_object(self):
        """Download the object to local storage."""
        local_path = self.local_path()
        # Debug logging
        self.provider.logger.debug(f"Retrieving {self.query} to {local_path}")
        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)
        # Download the file
        result = self._fs.download(self.query, str(local_path))
        self.provider.logger.debug(f"Retrieve result: {result}")
        self.provider.logger.debug(
            f"Local path exists after retrieve: {local_path.exists()}"
        )
        return result

    @retry_decorator
    def store_object(self):
        """Upload the object to storage."""
        local_path = self.local_path()
        # Debug logging
        self.provider.logger.debug(f"Storing {local_path} to {self.query}")
        self.provider.logger.debug(
            f"Local path exists before store: {local_path.exists()}"
        )

        # Ensure parent directory exists in the remote storage
        parent = str(Path(self.query).parent)
        if parent and parent != ".":
            self.provider.logger.debug(f"Creating remote directory: {parent}")
            self._fs.makedirs(parent, exist_ok=True)

        result = self._fs.upload(str(local_path), self.query)
        self.provider.logger.debug(f"Store result: {result}")
        return result

    @retry_decorator
    def remove(self):
        """Remove the object from storage."""
        # Use rm with recursive=True to handle both files and directories
        # This will remove directories even if they're not empty
        self._fs.rm(self.query, recursive=True)

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # Get the prefix before any wildcards
        prefix = self.query.split("{")[0]
        # List all objects in the directory
        try:
            return self._fs.glob(prefix + "*")
        except Exception:
            return []

    def touch(self) -> None:
        # [NOTE] Truncate sets file-size to 0 bytes, you don't want that...
        self._fs.touch(self.query, truncate=False)
