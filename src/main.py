from omegaconf import OmegaConf
from util import setup


def main():
    config = setup("config/config.yaml")

    print("Config:")
    print(OmegaConf.to_yaml(config))


if __name__ == "__main__":
    main()
