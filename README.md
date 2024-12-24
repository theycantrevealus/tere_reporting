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
[MONGO]
URI=<mongo_connection_url>

[RESULT]
DIR=<target_dir_report_result>
```

## How to Operate
### Fact Detail Generator
System will prompt report period ( `from_date` `to_date`) and then target file name. File will store into `DIR` target as configured on `.env` file at `RESULT` section

### 0POIN (DCI) Generator
System will prompt report period ( `from_date` `to_date`) and then target file name. File will store into `DIR` target as configured on `.env` file at `RESULT` section
