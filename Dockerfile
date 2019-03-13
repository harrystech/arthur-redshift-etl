#
# This attempts to create an environment as close to what ./bin/bootstrap.sh
# creates as possible with the images available. TODO: Resolve assumptions
# from bootstrap.sh so it can be used here to remove code duplication.
#
FROM amazonlinux:2017.03

WORKDIR /tmp/redshift_etl

RUN yum install -y \
        postgresql95-devel \
        python35 \
        python35-pip \
        python35-devel \
        aws-cli \
        gcc \
        libyaml-devel \
        tmux \
        jq && \
    pip-3.5 install --upgrade --disable-pip-version-check virtualenv


COPY requirements*.txt ./
RUN virtualenv --python=python3 venv && \
    source venv/bin/activate && \
    pip3 install --upgrade pip --disable-pip-version-check && \
    pip3 install --requirement ./requirements-dev.txt

COPY . .
RUN source venv/bin/activate && \
    python3 setup.py develop

# Use the self tests to check if everything was installed properly
RUN source venv/bin/activate && \
    run_tests.py

# Ensure the venv is activated when running interactive shells
RUN echo $'source /tmp/redshift_etl/venv/bin/activate\n\
source /tmp/redshift_etl/etc/arthur_completion.sh\n\
PATH=$PATH:/tmp/redshift_etl/bin' > /root/.bashrc

WORKDIR /data-warehouse

# From here, bind-mount your data warehouse code directory to /data-warehouse.
# All of the normal Arthur configuration for accessing and managing the data
# warehouse is assumed to have already been set up in that directory.