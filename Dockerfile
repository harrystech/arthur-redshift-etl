# This attempts to create an environment close to what ./bin/bootstrap.sh creates
# with the images available. Note that we install code (and the virtual environment)
# into /opt. There will be a copy of the source code in /arthur-redshift-etl. If you
# make changes, run "python setup.py develop"!
#
# N.B. Make sure to keep the Dockerfile and bootstrap script in sync wrt. packages.

FROM amazonlinux:2.0.20201218.1 AS local

# AWS Default Region so that building images doesn't stumble over a missing region information.
ARG AWS_DEFAULT_REGION=us-east-1
ENV AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION"

# See the same setting in bin/bootstrap.sh
ARG PROJ_NAME=redshift_etl

RUN yum install -y \
        awscli \
        git \
        jq \
        libyaml-devel \
        openssh-clients \
        passwd \
        postgresql \
        procps-ng \
        python3 \
        python3-devel \
        sudo \
        tar \
        tmux \
        vim-minimal

RUN amazon-linux-extras install docker

# Run as non-priviledged user "arthur".
RUN useradd --comment 'Arthur ETL' --user-group --groups wheel --create-home arthur  && \
    mkdir --parent /opt/data-warehouse "/opt/local/$PROJ_NAME" /opt/src/arthur-redshift-etl && \
    chown -R arthur.arthur /opt/*

RUN echo "arthur:arthur" | chpasswd

USER arthur

# The .bashrc will ensure the virutal environment is activated when running interactive shells.
COPY --chown=arthur:arthur docker/* /home/arthur/

# Install code under /opt/local/ (although it is under /tmp/ on an EC2 host).
COPY --chown=arthur:arthur \
    bin/create_validation_credentials \
    bin/release_version.sh \
    bin/send_health_check.sh \
    bin/sync_env.sh \
    bin/upload_env.sh \
    "/opt/local/$PROJ_NAME/bin/"

COPY requirements*.txt /tmp/
RUN python3 -m venv "/opt/local/$PROJ_NAME/venv" && \
    source "/opt/local/$PROJ_NAME/venv/bin/activate" && \
    python3 -m pip install --upgrade pip==20.3.4 --disable-pip-version-check --no-cache-dir && \
    python3 -m pip install --requirement /tmp/requirements-all.txt --disable-pip-version-check --no-cache-dir

# Create an empty .pgpass file to help with the format of this file.
RUN echo '# Format to set password when updating users: *:5439:*:<user>:<password>' > /home/arthur/.pgpass \
    && chmod go= /home/arthur/.pgpass

# Note that at runtime we (can or may) mount the local directory here.
# But we want to be independent of the source so copy everything over once.
WORKDIR /opt/src/arthur-redshift-etl
COPY --chown=arthur:arthur ./ ./

# We run this here once in case somebody overrides the entrypoint.
RUN source "/opt/local/$PROJ_NAME/venv/bin/activate" && \
    python3 setup.py install && \
    rm -rf build dist && \
    python3 -m etl.selftest && \
    arthur.py --version

# Whenever there is an ETL running, it offers progress information on port 8086.
EXPOSE 8086

# The data warehouse (with schemas, config, etc.) will be mounted here:
WORKDIR /opt/data-warehouse

ENTRYPOINT ["/home/arthur/entrypoint.sh"]
CMD ["/bin/bash", "--login"]

# Second stage, overriding entrypoint in the image to not have to override it everytime
# we use the Arthur image in a remote environment. entrypoint_remote.sh will fetch config
# and schema files from S3
FROM local AS remote
COPY --chown=arthur:arthur \
    bin/bootstrap_remote_dw.sh \
    "/opt/local/$PROJ_NAME/bin/"
ENV PROJ_NAME=$PROJ_NAME
ENTRYPOINT ["/home/arthur/entrypoint_remote.sh"]
