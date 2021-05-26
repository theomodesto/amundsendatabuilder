# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import databuilder
from typing import (
    Any, Dict, Iterator, List, Union,
)

import boto3
from pyhocon import ConfigFactory, ConfigTree

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata
from databuilder.models.table_last_updated import TableLastUpdated
from datetime import datetime


class GlueLastUpdatedExtractor(Extractor):
    """
    Extracts tables and columns metadata from AWS Glue metastore
    """

    CLUSTER_KEY = 'cluster'
    FILTER_KEY = 'filters'
    DEFAULT_CONFIG = ConfigFactory.from_dict({CLUSTER_KEY: 'gold', FILTER_KEY: None})

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(GlueLastUpdatedExtractor.DEFAULT_CONFIG)
        self._cluster = conf.get_string(GlueLastUpdatedExtractor.CLUSTER_KEY)
        self._filters = conf.get(GlueLastUpdatedExtractor.FILTER_KEY)
        self._glue = boto3.client('glue')
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[TableLastUpdated, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.glue'


    def _get_extract_iter(self) ->  Iterator[TableLastUpdated]:
        """
        It gets all tables and yields TableMetadata
        :return:
        """
        for row in self._get_raw_extract_iter():

            yield  TableLastUpdated(
               cluster=self._cluster,
               db='glue',
               schema=row['DatabaseName'],
               table_name=row['Name'],
               last_updated_time_epoch=int(row['UpdateTime'].timestamp())
            )

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        """
        Provides iterator of results row from glue client
        :return:
        """
        tables = self._search_tables()
        return iter(tables)

    def _search_tables(self) -> List[Dict[str, Any]]:
        tables = []
        kwargs = {}
        if self._filters is not None:
            kwargs['Filters'] = self._filters
        data = self._glue.search_tables(**kwargs)
        tables += data['TableList']
        while 'NextToken' in data:
            token = data['NextToken']
            kwargs['NextToken'] = token
            data = self._glue.search_tables(**kwargs)
            tables += data['TableList']
        return tables
