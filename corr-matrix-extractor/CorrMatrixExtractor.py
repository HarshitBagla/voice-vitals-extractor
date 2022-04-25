#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
from pyclowder.extractors import Extractor
import pyclowder.files
import pandas as pd


class CorrMatrixExtractor(Extractor):
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        self.parser.add_argument('--num', '-n', type=int, nargs='?', default=2,
                                 help='number of feature files to start compute correlation (default=2)')

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

        # get the input file name and check if it's correlation matrix itself; then skip
        trigger_file_name = resource["name"]
        if trigger_file_name != 'corrMat.csv':
            return

        dataset_id = resource['parent'].get('id')

        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")
        files_in_dataset = pyclowder.datasets.get_file_list(connector, host, secret_key, dataset_id)
        csvfiles_df = pd.DataFrame()

        # Making the corr Matrix once it reaches the num of files
        feature_files_in_dataset = [file for file in files_in_dataset if file["filename"].endswith("_summary.csv")]
        logger.debug("feature files number: " + str(len(feature_files_in_dataset)))
        if len(feature_files_in_dataset) >= self.args.num and len(feature_files_in_dataset) % self.args.num == 0:
            for file in feature_files_in_dataset:
                file_id = file["id"]
                curr_csvFile = pyclowder.files.download(connector, host, secret_key, file_id,
                                                        intermediatefileid=None, ext="csv")
                pd_currcsvFile = pd.read_csv(curr_csvFile)
                csvfiles_df = pd.concat([csvfiles_df, pd_currcsvFile]).apply(pd.to_numeric)

            temp_dfDisplay = csvfiles_df.iloc[:, :20]
            # logger.debug(temp_dfDisplay.head(5))

            corrMat = temp_dfDisplay.corr()
            # logger.debug(corrMat.head())

            # overwrite existing correlation matrix
            corrMat_file_name = 'corrMat.csv'
            for file in files_in_dataset:
                if file["filename"] == corrMat_file_name:
                    url = '%sapi/files/%s?key=%s' % (host, file["id"], secret_key)
                    connector.delete(url, verify=connector.ssl_verify if connector else True)
            corrMat.to_csv(corrMat_file_name)
            pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, corrMat_file_name)


if __name__ == "__main__":
    extractor = CorrMatrixExtractor()
    extractor.start()
