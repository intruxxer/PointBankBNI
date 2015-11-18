#!/bin/bash

# show commands being executed, per debug
set -x

# define database connectivity
_db="bnizona"
_db_user="root"
_db_password="BrynCahy0" #BrynCahy0 #af1988

# define directory containing CSV files
_csv_directory="files"

# go into directory
cd $_csv_directory

# get a list of CSV files in directory
_csv_files=`ls -1 *.txt`

# loop through csv files
for _csv_file in ${_csv_files[@]}
do

  # remove file extension
  _csv_file_extensionless=`echo $_csv_file | sed 's/\(.*\)\..*/\1/'`

  # define table name
  _generic_table_name="${_csv_file_extensionless}"

  # get header columns from CSV file
  # _header_columns=` head -1 $_csv_directory/$_csv_file | tr '|' '\n' | sed 's/^"//' | sed 's/"$//' | sed 's/ /_/g' `
  # _header_columns_string=` head -1 $_csv_directory/$_csv_file | sed 's/ /_/g' | sed 's/"//g' `

  # ensure table exists
  mysql -u $_db_user -p$_db_password $_db --execute="
    CREATE TABLE IF NOT EXISTS tbl_points_$_generic_table_name (
      point_id int(11) NOT NULL auto_increment,
	  point_no int(7) NOT NULL DEFAULT '0',
	  point_accnum varchar(20) NOT NULL DEFAULT '0',
	  point_cardno varchar(16) NOT NULL DEFAULT '0',
	  point_$_generic_table_name int(6) NOT NULL DEFAULT '0',
	  deleted int(1) NOT NULL DEFAULT '0',
	  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY  (point_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
  "
  mysql -u $_db_user -p$_db_password $_db --execute="TRUNCATE TABLE tbl_points_$_generic_table_name;"
  
  _csv_file_path=$_csv_file
  # loop through header columns
  # for _header in ${_header_columns[@]}
  # do

    # add column
    #  mysql -u $_db_user -p$_db_password $_db --execute="ALTER TABLE \`$_table_name\` ADD COLUMN \`$_header\` text"

  # done

  # import csv into mysql
  # cara 1: 
  # http://dev.mysql.com/doc/refman/5.7/en/mysqlimport.html
  # mysqlimport --fields-enclosed-by='' --fields-terminated-by='|' --lines-terminated-by="\r\n" --ignore-lines=1 -u $_db_user -p$_db_password $_db $_csv_file_path
   
  # cara 2: 
  # http://dev.mysql.com/doc/refman/5.7/en/load-data.html
    mysql -u $_db_user -p$_db_password $_db -e "
  		   LOAD DATA LOCAL INFILE '$_csv_file_path' 
           INTO TABLE tbl_points_$_generic_table_name 
           FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES
           (point_no, point_cardno, point_$_generic_table_name);"

done
exit