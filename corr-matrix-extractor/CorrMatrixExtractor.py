#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
from pyclowder.extractors import Extractor
import pyclowder.files
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


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
        # this extractor runs on dataset
        # uncomment to see the resource
        logger = logging.getLogger(__name__)
        dataset_id = resource['id']

        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")
        files_in_dataset = pyclowder.datasets.get_file_list(connector, host, secret_key, dataset_id)
        csvfiles_df = pd.DataFrame()

        # Making the corr Matrix once it reaches the num of files
        feature_files_in_dataset = [file for file in files_in_dataset if file["filename"].endswith("_summary.csv")]
        logger.debug("feature files number: " + str(len(feature_files_in_dataset)))
        if len(feature_files_in_dataset) >= self.args.num:
            for file in feature_files_in_dataset:
                file_id = file["id"]
                curr_csvFile = pyclowder.files.download(connector, host, secret_key, file_id,
                                                        intermediatefileid=None, ext="csv")
                pd_currcsvFile = pd.read_csv(curr_csvFile)
                csvfiles_df = pd.concat([csvfiles_df, pd_currcsvFile]).apply(pd.to_numeric)

            aggregated_features = csvfiles_df.iloc[:, :20]
            # logger.debug(aggregated_features.head(5))

            corrMat = aggregated_features.corr()
            # logger.debug(corrMat.head())

            # overwrite existing aggregated features
            features_file_name = 'aggregatedFeatures.csv'
            for file in files_in_dataset:
                if file["filename"] == features_file_name:
                    url = '%sapi/files/%s?key=%s' % (host, file["id"], secret_key)
                    connector.delete(url, verify=connector.ssl_verify if connector else True)
            aggregated_features.to_csv(features_file_name)
            pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, features_file_name)

            # overwrite existing correlation matrix
            corrMat_file_name = 'corrMat.csv'
            for file in files_in_dataset:
                if file["filename"] == corrMat_file_name:
                    url = '%sapi/files/%s?key=%s' % (host, file["id"], secret_key)
                    connector.delete(url, verify=connector.ssl_verify if connector else True)
            corrMat.to_csv(corrMat_file_name)
            corrMat_file_id = pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id,
                                                                corrMat_file_name)

            # plot correlation matrix and attach to preview
            preview_filename_corr = "corrMat_heatmap.png"
            matrix = corrMat.round(2)
            plt.cla()
            sns.set(rc={'figure.figsize': (20, 15)})
            sns.heatmap(matrix, annot=False)
            plt.savefig(preview_filename_corr)
            pyclowder.files.upload_preview(connector, host, secret_key, corrMat_file_id, preview_filename_corr)


if __name__ == "__main__":
    extractor = CorrMatrixExtractor()
    extractor.start()
