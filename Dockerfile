FROM python:3
RUN apt-get update && apt-get install jq -q
WORKDIR "/arthur"
COPY requirements.txt ./
COPY requirements-dev.txt ./
RUN pip install --requirement ./requirements-dev.txt
COPY . .
RUN [ "python", "setup.py", "develop" ]
WORKDIR "/hdw"
ENTRYPOINT ["arthur.py"]
