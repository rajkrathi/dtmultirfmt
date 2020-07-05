# Ingest data from single source file containing multiple record layout
## dtmultirfmt : dt - Data  |  multirfmt - Multiple record format
<ul>
<li>Python code reads one source file with multiple fixed width records</li>
<li>Based on record type, data is mapped to desired output record format</li>
<li>Creates multiple output files - one type of recor is distributed to one file</li>
<li>Perform data transformation remove space, remove delimeter</li>
<li>Write records to output files </li>
  <li>Read data from newly created files in data frame </li>
  <li>Write dataframes to SQL tables</li>
  <li>Execute any transformation Stored procedure on sql databse</li>
</ul>
 