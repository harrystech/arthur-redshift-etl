# This is the default .bashrc file inside a container.

PS1='(aws:$AWS_PROFILE, prefix=$ARTHUR_DEFAULT_PREFIX) \$ '

source /tmp/redshift_etl/venv/bin/activate
PATH=$PATH:/tmp/redshift_etl/bin

# Useful when developing Arthur
alias develop="( \cd /arthur-redshift-etl && python setup.py develop )"

source /arthur-redshift-etl/etc/arthur_completion.sh

# TODO(tom): This should be in a .bash_profile?
cat /arthur-redshift-etl/etc/motd
echo -e "\nEnvironment settings:\n"
arthur.py settings object_store.s3.* version
echo
