#
# This attempts to create an environment as close as possible to what ./bin/bootstrap.sh
# creates with the images available. One notable exception is that the code
# is expected in /arthur-redshift-etl and is not copied into /tmp/redshift_etl
# N.B. Make sure to keep this and the bootstrap script in sync!
#
FROM amazonlinux:2017.03

RUN yum install -y \
        aws-cli \
        gcc \
        jq \
        libyaml-devel \
        openssh-clients \
        postgresql95-devel \
        python35 \
        python35-devel \
        python35-pip \
        tmux \
        vim-minimal \
    && \
    pip-3.5 install --upgrade --disable-pip-version-check virtualenv

WORKDIR /tmp/redshift_etl

COPY requirements*.txt ./

RUN virtualenv --python=python3 venv && \
    source venv/bin/activate && \
    pip3 install --upgrade pip --disable-pip-version-check && \
    pip3 install --requirement ./requirements-dev.txt

COPY bin/release_version.sh bin/send_health_check.sh bin/sync_env.sh bin/upload_env.sh bin/

WORKDIR /arthur-redshift-etl

# Note that at runtime we (can or may) mount the local directory here.
# But we want to be independent of the source so copy everything over once.
COPY . ./

# Use the self tests to check if everything was installed properly
RUN source /tmp/redshift_etl/venv/bin/activate && \
    python3 setup.py develop && \
    run_tests.py

# The .bashrc will ensure the venv is activated when running interactive shells.
RUN cat /arthur-redshift-etl/etc/default.bashrc > /root/.bashrc

# Create an empty .pgpass file to help with create_user and update_user commands.
RUN echo '# Format to set password (used by create_user and update_user): *:5439:*:<user>:<password>' > /root/.pgpass \
    && chmod go= /root/.pgpass

# Whenever there is an ETL running, it offers progress information on port 8086.
EXPOSE 8086

WORKDIR /data-warehouse
# From here, bind-mount your data warehouse code directory to /data-warehouse.
# All of the normal Arthur configuration for accessing and managing the data
# warehouse is assumed to have already been set up in that directory.

ENTRYPOINT ["/arthur-redshift-etl/bin/entrypoint.sh"]
CMD ["/bin/bash"]
