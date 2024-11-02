FROM gcr.io/dataflow-templates-base/python3-template-launcher-base
arg BASE=/template
arg PY_FILE=logging_ex.py
arg REQUIREMENTS_FILE=requirements.txt
arg SETUP=setup.py
arg DEPENDENCIES=dependencies

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${BASE}/${PY_FILE}"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${BASE}/$SETUP

COPY ${PY_FILE} ${BASE}/${PY_FILE}
COPY ${REQUIREMENTS_FILE} ${BASE}/${REQUIREMENTS_FILE}
COPY ${DEPENDENCIES} ${BASE}/${DEPENDENCIES}
COPY ${SETUP} ${BASE}


RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r ${BASE}/${REQUIREMENTS_FILE} \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r ${BASE}/${REQUIREMENTS_FILE}

# Set this if using Beam 2.37.0 or earlier SDK to speed up job submission.
ENV PIP_NO_DEPS=True

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]

