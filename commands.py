#!/usr/bin/env python
"""

Version 0.1
2018-07-31
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import argparse

from sqlalchemy import DDL

from health_tracker_config import engine, schema_name
from models import SABase


def create_tables():
    engine.execute(DDL('CREATE SCHEMA IF NOT EXISTS {schema}'.format(
        schema=schema_name,
    )))
    SABase.metadata.create_all()

def drop_tables():
    SABase.metadata.drop_all()


def run_main():
    args = parse_cl_args()

    if args.create_tables:
        create_tables()
    elif args.drop_tables:
        drop_tables()
    else:
        print('no action specified')

    success = True
    return success

def parse_cl_args():
    argParser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    argParser.add_argument(
        '--create-tables',
        default=False,
        action='store_true',
    )
    argParser.add_argument(
        '--drop-tables',
        default=False,
        action='store_true',
    )

    args = argParser.parse_args()
    return args

if __name__ == '__main__':
    success = run_main()
    exit_code = 0 if success else 1
    exit(exit_code)
