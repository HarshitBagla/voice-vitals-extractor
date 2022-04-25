# Base image is an Ubuntu based
FROM python:3

# Creating workdir
WORKDIR /home/clowder

# Install pyClowder and any other python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Adding necessary code to container under workdir
COPY CorrMatrixExtractor.py extractor_info.json /home/clowder/

# Command to be run when container is run
# Can add heartbeat to change the refresh rate
CMD python3 CorrMatrixExtractor.py --heartbeat 40

ENV MAIN_SCRIPT="CorrMatrixExtractor.py"