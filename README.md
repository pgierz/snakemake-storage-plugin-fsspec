# Snakemake Storage Plugin - FSSpec

This Snakemake storage plugin provides integration with fsspec-based storage systems.

## Usage

### In Snakefiles

To use this plugin in your Snakefile, simply prefix your file paths with `fsspec://`. The plugin will automatically handle the storage operations using the configured protocol.

Example:
```python
rule example:
    input:
        "fsspec://file://input.txt",
    output:
        "fsspec://file://output.txt",
    shell:
        "cp {input} {output}"
```

### Configuration

The plugin can be configured either through command line parameters or in a config file.

#### Command Line

```bash
snakemake --storage-fsspec-protocol s3  # Use S3 protocol
```

#### Config File

You can also configure the plugin through a YAML config file:
```yaml
storage:
  fsspec:
    protocol: "file"  # Default protocol
    # Additional settings can be added here
```

### Supported Operations

The plugin supports the following operations:
- Reading from storage
- Writing to storage
- Removing files
- Listing files (for wildcard patterns)
- Checking file existence
- Getting file size and modification time

### Example Workflow

See the `Snakefile` in this repository for a complete example workflow that demonstrates:
- Basic file operations
- Wildcard pattern matching
- Multiple input files

### Installation

Install the plugin using pip:
```bash
pip install snakemake-storage-plugin-fsspec
```

### Requirements

- Python 3.8+
- Snakemake 8.0+
- fsspec