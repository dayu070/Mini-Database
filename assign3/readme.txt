525 assignment3 record manager
Team members: Qi Zhan, Jingyu Zhu, Qianqi Guan
------------------------------------------------------------------------------------------------------------------------

main implementation is in record_mgr.c. Major functionalities are for operating tables, schemas and records of a database. 

we added some functions like serialSchema(), getRecordHeaders() etc. for convenience of other function's invokes.  

extra feature:
We implemented null values as assignment extension.  We store a record header which contains bool isDeleted and bool *isNULL(indicates whether each attribute is null or not). 
Every time we insert/update/delete record we took the record header size into consideration. 