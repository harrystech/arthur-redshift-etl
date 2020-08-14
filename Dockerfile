# This attempts to create an environment close to what ./bin/bootstrap.sh creates
# with the images available. Note that we install code (and the virtual environment)
# into /opt. There will be a copy of the source code in /arthur-redshift-etl. If you
# make changes, run "python setup.py develop"!
#
# N.B. Make sure to keep the Dockerfile and bootstrap script in sync wrt. packages.

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

# Run as non-priviledged user "arthur".
RUN useradd --comment 'Arthur ETL' --user-group --create-home arthur && \
    mkdir --parent /opt/data-warehouse /opt/local/redshift_etl /opt/src/arthur-redshift-etl && \
    chown -R arthur.arthur /opt/*
USER arthur

# The .bashrc will ensure the virutal environment is activated when running interactive shells.
COPY --chown=arthur:arthur etc/default.bashrc /home/arthur/.bashrc
COPY --chown=arthur:arthur etc/default.bash_profile /home/arthur/.bash_profile

# Install code in /opt/local/redshift_etl (which would be in /tmp/redshift_etl on an EC2 host).
COPY --chown=arthur:arthur \
    bin/release_version.sh bin/send_health_check.sh bin/sync_env.sh bin/upload_env.sh \
    /opt/local/redshift_etl/bin/

COPY requirements*.txt /tmp/
RUN virtualenv --python=python3 /opt/local/redshift_etl/venv && \
    source /opt/local/redshift_etl/venv/bin/activate && \
    pip3 install --upgrade pip --disable-pip-version-check --no-cache-dir && \
    pip3 install --requirement /tmp/requirements-dev.txt --disable-pip-version-check --no-cache-dir

# Create an empty .pgpass file to help with create_user and update_user commands.
RUN echo '# Format to set password (used by create_user and update_user): *:5439:*:<user>:<password>' > /home/arthur/.pgpass \
    && chmod go= /home/arthur/.pgpass

# Note that at runtime we (can or may) mount the local directory here.
# But we want to be independent of the source so copy everything over once.
WORKDIR /opt/src/arthur-redshift-etl
COPY --chown=arthur:arthur ./ ./

# Ww run this here once in case somebody overrides the entrypoint.
RUN source /opt/local/redshift_etl/venv/bin/activate && \
    python3 setup.py install && \
    rm -rf build dist && \
    arthur.py --version


# Whenever there is an ETL running, it offers progress information on port 8086.
EXPOSE 8086

# The data warehouse (with schemas, config, etc.) will be mounted here:
WORKDIR /opt/data-warehouse

ENTRYPOINT ["/opt/src/arthur-redshift-etl/bin/entrypoint.sh"]
CMD ["/bin/bash"]
