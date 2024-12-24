# TERE Manual Report Generator
This is python script to generate report manually in case of special condition such like too big report data that can't be handled by NodeJS

## Getting Started
### Install Packages
Run following command to install all requirements
```shell
  pip install -r /project_root_path/requirements.txt
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
### Fact Detail Generator
System will prompt report period ( `from_date` `to_date`) and then target file name. File will store into `DIR` target as configured on `.env` file at `RESULT` section

### 0POIN (DCI) Generator
System will prompt report period ( `from_date` `to_date`) and then target file name. File will store into `DIR` target as configured on `.env` file at `RESULT` section
