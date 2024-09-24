FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

#ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/wordcount.py"

COPY wordcount.py /template/wordcount.py
COPY requirements.txt /template/requirements.txt


RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /template/requirements.txt \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r /template/requirements.txt

# Set this if using Beam 2.37.0 or earlier SDK to speed up job submission.
ENV PIP_NO_DEPS=True
ENV TEST_VAR=TEST1

COPY dist/mypackagename-1.0.0.tar.gz .
RUN pip install mypackagename-1.0.0.tar.gz

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]

