# This is the default .bash_profile in a container.

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
    . ~/.bashrc
fi

# Initial greeting
cat /opt/src/arthur-redshift-etl/etc/motd
echo -e "\nEnvironment settings:\n"
arthur.py settings object_store.s3.* version
echo
