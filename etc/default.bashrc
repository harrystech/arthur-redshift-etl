# This is the default .bashrc file inside a container.

PS1='(aws:$AWS_PROFILE, prefix=$ARTHUR_DEFAULT_PREFIX) \$ '

source /opt/redshift_etl/venv/bin/activate
PATH=$PATH:/opt/redshift_etl/bin

# Useful when developing Arthur
alias develop="( \cd /arthur-redshift-etl && python setup.py develop )"

# Generally useful
alias ll='ls -alF'

source /arthur-redshift-etl/etc/arthur_completion.sh

cat /arthur-redshift-etl/etc/motd
echo -e "\nEnvironment settings:\n"
arthur.py settings object_store.s3.* version
echo
