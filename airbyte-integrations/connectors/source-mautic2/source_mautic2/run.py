#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceMautic2

def run():
    source = SourceMautic2()
    launch(source, sys.argv[1:])
