# TERE Manual Report Generator
This is python script to generate report manually in case of special condition such like too big report data that can't be handled by NodeJS

## Getting Started
### Install Packages
Run following command to install all requirements
```shell
  pip install -r /project_root_path/requirements.txt
```

### Using Virtual Environment
```shell
  python3 -m venv <root_project>/venv

  source <root_project>/venv/bin/activate
```

### Environment Configuration
Create `.env` file contains following field
```env
[ENVIRONMENT]
TARGET=development

[CONFIG]
BATCH_SIZE=10000

[MONGO]
URI=mongodb://127.0.0.1:27117,127.0.0.1:27118/SLRevamp2
HOST=<server1>:<port1>,<server2>:<port2>
USERNAME=<db_username>
PASSWORD=<db_password>
DATABASE=<db_name>
EXTRA=authSource=admin&tls=true&tlsAllowInvalidCertificates=true&connectTimeoutMS=10000&anotherKey=anotherValue

[RESULT]
DIR=./report
```

## How to Operate
```shell

    ./venv/bin/python3 index.py

```