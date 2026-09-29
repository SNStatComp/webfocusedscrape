import logging

from omegaconf import OmegaConf


def setup(config_path):
    # Read config object
    config = OmegaConf.load(config_path)
    return config


# Read an input file listed under input.input_files, one stripped line per entry
def read_input_file(config, input_file, label):
    file_path = f"{config.input.input_dir}/{config.input.input_files[input_file]}"
    logging.info(f"Reading list of {label} from file: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as file_in:
        return [line.rstrip() for line in file_in]
