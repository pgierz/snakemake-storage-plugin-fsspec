storage tape_archive:
  provider="fsspec",
  protocol="sftp",
  storage_options={'host': 'hsm.dmawi.de',},

        
rule example_local_input_remote_output:
    output: storage.tape_archive("remote-file.txt")
    input: "local-file.txt"
    script: "../scripts/remote_output.py"

rule example_script_local_output:
    output: "local-file.txt"
    script: "../scripts/local_output.py"


rule example_remote_input_local_output:
    output: 'local-copy.txt'
    input: hsm_file = storage.tape_archive("remote-file.txt")
    script: "../scripts/remote_input_local_output.py"
