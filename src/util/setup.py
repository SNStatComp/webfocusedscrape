from omegaconf import OmegaConf


def setup(config_path):
    # Read config object
    config = OmegaConf.load(config_path)
    return config
