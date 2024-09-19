#FROM apache/beam_python3.10_sdk:2.58.1
FROM apache/beam_python3.8_sdk:2.57.0

COPY requirements.txt .
COPY dist/mypackagename-1.0.0.tar.gz .
RUN pip install mypackagename-1.0.0.tar.gz
RUN pip install -r requirements.txt
#install test var on image for use in Beam
ENV TEST_VAR=TEST1

