# Reformatting code

Changing from 120 chars to 100 chars.

## Step 1: fix up all the comments

1. Reformat code and commit.
```shell script
black --config ./etc/future_pyproject.toml python/
git add -vu
git commit --no-verify -m 'reformat code'
```
1. Look for violations in comments & fix them. (Only change comments at this time.)
```shell script
flake8 --config ./etc/future_setup.cfg python/
```
1. Commit and rebase to remove step 1.
