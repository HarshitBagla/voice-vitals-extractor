#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import os
from pyclowder.extractors import Extractor
import pyclowder.files
import opensmile
import seaborn
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy.io.wavfile as wavfile
import pylab


class OpenSmileExtractor(Extractor):
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
        inputfile = resource["local_paths"][0]
        file_id = resource['id']

        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")

        # Call actual program
        # Execute word count command on the input file and obtain the output
        smile = opensmile.Smile(
            feature_set=opensmile.FeatureSet.ComParE_2016,
            feature_level=opensmile.FeatureLevel.Functionals,
        )

        # 1. Create metadata dictionary
        y = smile.process_file(inputfile)

        m = y.to_dict('records')[0]
        result = {
            'audspec_lengthL1norm_sma_range': m['audspec_lengthL1norm_sma_range'],
            'audspec_lengthL1norm_sma_maxPos': m['audspec_lengthL1norm_sma_maxPos'],
            'audspec_lengthL1norm_sma_minPos': m['audspec_lengthL1norm_sma_minPos']
        }
        # connector.message_process(resource, "Found %s lines and %s words..." % (lines, words))

        # Store results as metadata
        metadata = self.get_metadata(result, 'file', file_id, host)

        # Normal logs will appear in the extractor log, but NOT in the Clowder UI.
        logger.debug(metadata)

        # Upload metadata to original file
        pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)

        # 2. store table as new file and upload
        original_filename = resource["name"]
        filename = os.path.splitext(original_filename)[0] + "_summary.csv"
        y.to_csv(filename, index=False)
        dataset_id = resource['parent'].get('id')
        pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, filename)

        # # 3. store as preview
        # # Do all the data viz stuff here
        # Fs, aud = wavfile.read(inputfile)
        # # powerSpectrum, frequenciesFound, trim, imageAxis = plt.specgram(aud, Fs=Fs)
        
        # # Builting the spectrogram
        # NFFT = int(Fs*0.005)  # 5ms window
        # noverlap = int(Fs*0.0025)
        # pylab.specgram(aud, NFFT=NFFT, Fs=Fs, noverlap=noverlap)
        # pylab.colorbar()
        
        # # Making the 2000 Hz Spectrogram
        # preview_filename_2000 = os.path.splitext(original_filename)[0] + "_specgram2000.png"
        # pylab.ylim(0, 2000)
        # pylab.savefig(preview_filename_2000)
        # pyclowder.files.upload_preview(connector, host, secret_key, file_id, preview_filename_2000)
        
        # # Making the 4000 Hz Spectrogram
        # preview_filename_4000 = os.path.splitext(original_filename)[0] + "_specgram4000.png"
        # pylab.ylim(0, 4000)
        # pylab.savefig(preview_filename_4000)
        # pyclowder.files.upload_preview(connector, host, secret_key, file_id, preview_filename_4000)
        
        # # Making the 6000 Hz Spectrogram
        # preview_filename_6000 = os.path.splitext(original_filename)[0] + "_specgram6000.png"
        # pylab.ylim(0, 6000)
        # pylab.savefig(preview_filename_6000)
        # pyclowder.files.upload_preview(connector, host, secret_key, file_id, preview_filename_6000)
        
        # Making the corr Matrix after every file upload
        files_in_dataset = pyclowder.datasets.get_file_list(connector, host, secret_key, dataset_id)
        csvfiles_df = pd.DataFrame()
        for file in files_in_dataset:
            file_id = file["id"]
            if ".csv" not in file["filename"]:
                continue
            curr_csvFile = pyclowder.files.download(connector, host, secret_key, file_id, intermediatefileid=None, ext="csv")
            pd_currcsvFile = pd.read_csv(curr_csvFile)
            csvfiles_df = csvfiles_df.append(pd_currcsvFile)
        temp_dfDisplay = csvfiles_df.iloc[:, :20]
        corrMat = temp_dfDisplay.corr()
        corrMat_fileName = './corrMat.csv'
        corrMat.to_csv(corrMat_fileName)
        pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, corrMat_fileName)
        

if __name__ == "__main__":
    extractor = OpenSmileExtractor()
    extractor.start()
