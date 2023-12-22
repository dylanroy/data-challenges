import argparse


class ArgumentParser(object):
    def __init__(self):
        self._parser = argparse.ArgumentParser(description="Parse Variables")

    def get_arguments(self) -> [str]:
        self._setup_default_arguments()
        args, unknown_args = self._parser.parse_known_args()
        known_arguments = args.__dict__
        unknown_arguments = unknown_args
        return known_arguments, unknown_arguments

    def _setup_default_arguments(self) -> None:
        self._parser.add_argument(
            "--rank-field",
            required=True,
            dest="rank_field",
            help="The field to be ranked & ordered by date time most recent",
        )
