#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import os
from pyclowder.extractors import Extractor
import pyclowder.files
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class CorrMatrixExtractor(Extractor):
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, host, secret_key, resource, parameters):
        # Process the file and upload the results
        # uncomment to see the resource
        # print(resource)

        logger = logging.getLogger(__name__)
        dataset_id = resource['parent'].get('id')

        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")
        
        # Making the corr Matrix after every file upload
        files_in_dataset = pyclowder.datasets.get_file_list(connector, host, secret_key, dataset_id)
        csvfiles_df = pd.DataFrame()
        for file in files_in_dataset:
            file_id = file["id"]
            # Read only csv types
            if ".csv" in file["filename"]:
                # overwrite corrMat
                if file["filename"] == 'corrMat.csv':
                    url = '%sapi/files/%s?key=%s' % (host, file["id"], secret_key)
                    connector.delete(url, verify=connector.ssl_verify if connector else True)
                else:
                    curr_csvFile = pyclowder.files.download(connector, host, secret_key, file_id, intermediatefileid=None, ext="csv")
                    pd_currcsvFile = pd.read_csv(curr_csvFile)
                    csvfiles_df = pd.concat([csvfiles_df, pd_currcsvFile]).apply(pd.to_numeric)
        temp_dfDisplay = csvfiles_df.iloc[:, :20]
        # logger.debug(temp_dfDisplay.head(5))
        corrMat = temp_dfDisplay.corr()
        # logger.debug(corrMat.head())
        corrMat_fileName = 'corrMat.csv'
        corrMat.to_csv(corrMat_fileName)
        pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, corrMat_fileName)
        

if __name__ == "__main__":
    extractor = CorrMatrixExtractor()
    extractor.start()
